import datetime
import io
import json
import logging
import os
import sys

import orthanc  # pyright: ignore
import pydicom
import requests


# For acquisition date/time parsing
pydicom.config.datetime_conversion = True


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler(sys.stdout))


def _get_required_envvar(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        raise RuntimeError(f"{key} environment variable required but missing")


def _get_api_server_url() -> str:
    host = _get_required_envvar("API_SERVER_HOST")
    port = _get_required_envvar("API_SERVER_PORT")
    return f"{host}:{port}"


def _get_request_headers() -> dict:
    return {"Authorization": _get_required_envvar("API_TOKEN")}


def on_change(change_type, _, resourceId):
    if change_type == orthanc.ChangeType.STABLE_SERIES:
        if "ModifiedFrom" in json.loads(
            orthanc.RestApiGet(f"/series/{resourceId}")
        ):
            LOG.info(
                f"Skipping series {resourceId} because it generated "
                f"from a series description update"
            )
            return
        on_stable_series_change(resourceId)


def on_stable_series_change(series_resource_id: str):
    post_task_create_request(series_resource_id)


def _get_metadata(endpoint: str) -> dict:
    # This function just wraps the REST API endpoint exception
    # with a nicer message.
    try:
        return json.loads(orthanc.RestApiGet(endpoint)).get("MainDicomTags", {})
    except Exception:
        LOG.error(f"Error fetch series metadata: {endpoint}")
        raise


def post_task_create_request(series_resource_id: str):
    """POST to API server to create (or append to) study task for this series"""

    series_meta = _get_metadata(f"/series/{series_resource_id}")
    study_meta = _get_metadata(f"/series/{series_resource_id}/study")
    patient_meta = _get_metadata(f"/series/{series_resource_id}/patient")

    # This is a bit of tom foolery. This metadata is set at the end of
    # reformatting by the reformat engine and is used to tuck the series into
    # the task that started the processing.
    try:
        reformat_batch_task_id = int(
            orthanc.RestApiGet(
                f"/series/{series_resource_id}/metadata/ReformatBatchTaskId"
            ).decode()
        )
        reformat_group_task_id = int(
            orthanc.RestApiGet(
                f"/series/{series_resource_id}/metadata/ReformatGroupTaskId"
            ).decode()
        )
        reformat_id = int(
            orthanc.RestApiGet(
                f"/series/{series_resource_id}/metadata/ReformatId"
            ).decode()
        )
    except orthanc.OrthancException:
        reformat_batch_task_id = None
        reformat_group_task_id = None
        reformat_id = None

    try:
        instances: list[str] = json.loads(
            orthanc.RestApiGet(f"/series/{series_resource_id}")
        ).get("Instances", [])
        n_instances = len(instances)
    except Exception:
        LOG.error(
            f"Error fetching instance metadata for series {series_resource_id}"
        )
        raise

    # SeriesMetadata db model (somewhat confusingly
    # includes study, patient, etc. metadata)
    series_metadata_obj = {
        "orthanc_id": series_resource_id,
        "n_slices": n_instances,
        "dicom_metadata": {**series_meta, **study_meta, **patient_meta},
        "reformat_batch_task_id": reformat_batch_task_id,
        "reformat_group_task_id": reformat_group_task_id,
        "reformat_id": reformat_id,
    }

    # Add instance-level metadata

    acquisition_datetime = None
    slice_thickness = None
    convolution_kernel = None
    for instance_id in instances:
        f = orthanc.GetDicomForInstance(instance_id)
        # Parse it using pydicom
        dicom = pydicom.dcmread(
            io.BytesIO(f),
            specific_tags=[
                (0x0008, 0x002A),  # AcquisitionDateTime
                (0x0008, 0x0022),  # AcquisitionDate
                (0x0008, 0x0032),  # AcquisitionTime
                (0x0018, 0x0050),  # SliceThickness
                (0x0018, 0x1210),  # ConvolutionKernel
            ],
        )

        if slice_thickness is None and hasattr(dicom, "SliceThickness"):
            slice_thickness = str(dicom.SliceThickness)

        if convolution_kernel is None and hasattr(dicom, "ConvolutionKernel"):
            convolution_kernel = dicom.ConvolutionKernel

        if hasattr(dicom, "AcquisitionDateTime"):
            temp = dicom.AcquisitionDateTime
            # If acq unset or less than current, set it
            if acquisition_datetime is None or temp < acquisition_datetime:
                acquisition_datetime = temp
        elif hasattr(dicom, "AcquisitionDate") and hasattr(
            dicom, "AcquisitionTime"
        ):
            temp = pydicom.valuerep.DT(
                datetime.datetime.combine(
                    dicom.AcquisitionDate, dicom.AcquisitionTime
                )
            )
            # If acq unset or less than current, set it
            if acquisition_datetime is None or temp < acquisition_datetime:
                acquisition_datetime = temp
        # NOTE: we don't handle the case where AcquisitionTime is present
        # but AcquisitionDate is not. This is because we cannot sufficiently
        # determine the earliest timestamp in this case.

    if acquisition_datetime is not None:
        series_metadata_obj["dicom_metadata"][
            "AcquisitionDateTime"
        ] = acquisition_datetime.isoformat()
        series_metadata_obj["dicom_metadata"]["AcquisitionDate"] = str(
            pydicom.valuerep.DA(acquisition_datetime.date())
        )
        series_metadata_obj["dicom_metadata"]["AcquisitionTime"] = str(
            pydicom.valuerep.TM(acquisition_datetime.time())
        )
    else:
        for key in (
            "AcquisitionDateTime",
            "AcquisitionDate",
            "AcquisitionTime",
        ):
            series_metadata_obj["dicom_metadata"][key] = "N/A"

    series_metadata_obj["dicom_metadata"]["SliceThickness"] = (
        slice_thickness if slice_thickness else "N/A"
    )
    series_metadata_obj["dicom_metadata"]["ConvolutionKernel"] = (
        # str(kernel) to handle case when kernel is multi-value
        str(convolution_kernel)
        if convolution_kernel
        else "N/A"
    )

    response = requests.put(
        _get_api_server_url() + "/api/reformat-batch-task/create",
        json={"series_metadata": [series_metadata_obj]},
        headers=_get_request_headers(),
    )
    if response.status_code == 409:
        LOG.info(
            f"ReformatBatchTask exists for this series. Adding "
            f"series {series_resource_id} to existing task."
        )
        # 409 == resource conflict -- a task already exists
        # for data associated with this DICOM series (e.g., study),
        # so we instead create a SeriesMetadata instance with a
        # foreign key linking it to the respective ReformatBatchTask
        batch_task_id = response.json()["id"]
        response = requests.put(
            _get_api_server_url() + "/api/series-metadata/create",
            json={
                **series_metadata_obj,
                "reformat_batch_task_id": batch_task_id,
            },
            headers=_get_request_headers(),
        )
    elif not response.ok:
        raise RuntimeError(
            f"Error from API server for task creation: {response.status_code}"
        )


orthanc.RegisterOnChangeCallback(on_change)
