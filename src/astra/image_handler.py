from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
from alpaca.camera import ImageMetadata
from astropy.io import fits
from astropy.wcs.utils import WCS

from astra import Config

CONFIG = Config()


def create_image_dir(
    schedule_start_time: datetime = datetime.now(UTC),
    site_long: float = 0,
    user_specified_dir: Optional[str] = None,
) -> Path:
    """
    Create a directory to store images.

    This function creates a directory to store images. If a user-specified directory 
    is provided, it is used. Otherwise, the directory is created in the 'images' 
    folder with a name based on the schedule's beginning date (shifted to local 
    time using site's longitude).

    Parameters
    ----------
    schedule_start_time : datetime, optional
        The start time of the observing schedule, by default datetime.now(UTC)
    site_long : float, optional
        Site longitude in degrees (used to convert UTC to local time), by default 0
    user_specified_dir : str or None, optional
        Custom directory path to use instead of auto-generated path, by default None

    Returns
    -------
    Path
        Path object pointing to the created directory

    Notes
    -----
    The auto-generated directory name format is YYYYMMDD based on the local date
    calculated from schedule_start_time + (site_long / 15) hours.
    """

    if user_specified_dir:
        folder = Path(user_specified_dir)
        folder.mkdir(exist_ok=True)
    else:
        date_str = (schedule_start_time + timedelta(hours=site_long / 15)).strftime(
            "%Y%m%d"
        )
        folder = CONFIG.paths.images / date_str
        folder.mkdir(exist_ok=True)
    return folder


def transform_image_to_array(
    image: Union[List[int], np.ndarray], maxadu: int, image_info: ImageMetadata
) -> np.ndarray:
    """
    Transform image data to a numpy array with the correct shape and data type for FITS files.

    This function takes raw image data and metadata, determines the appropriate data type
    based on the image element type and maximum ADU value, and reshapes the array for
    compatibility with astropy.io.fits conventions.

    Parameters
    ----------
    image : list of int or np.ndarray
        Raw image data as a list or numpy array
    maxadu : int
        Maximum ADU (Analog-to-Digital Unit) value for the image
    image_info : ImageMetadata
        Metadata object containing image properties including:
        - ImageElementType: Data type indicator (0-3)
        - Rank: Number of dimensions (2 for grayscale, 3 for color)

    Returns
    -------
    np.ndarray
        Properly shaped and typed numpy array ready for FITS file creation.
        For 2D images: transposed array
        For 3D images: transposed with axes (2, 1, 0)

    Raises
    ------
    ValueError
        If ImageElementType is not in the expected range (0-3)

    Notes
    -----
    ImageElementType mapping:
    - 0, 1: uint16
    - 2: uint16 (if maxadu <= 65535) or int32 (if maxadu > 65535)
    - 3: float64
    
    The transpose operations are required to match FITS file conventions
    where the first axis corresponds to columns and the second to rows.
    """
    if not isinstance(image, np.ndarray):
        image = np.array(image)

    # Determine the image data type
    if image_info.ImageElementType == 0 or image_info.ImageElementType == 1:
        imgDataType = np.uint16
    elif image_info.ImageElementType == 2:
        if maxadu <= 65535:
            imgDataType = np.uint16  # Required for BZERO & BSCALE to be written
        else:
            imgDataType = np.int32
    elif image_info.ImageElementType == 3:
        imgDataType = np.float64
    else:
        raise ValueError(f"Unknown ImageElementType: {image_info.ImageElementType}")

    # Make a numpy array of the correct shape for astropy.io.fits
    if image_info.Rank == 2:
        image_array = np.array(image, dtype=imgDataType).transpose()
    else:
        image_array = np.array(image, dtype=imgDataType).transpose(2, 1, 0)

    return image_array


def save_image(
    image: Union[List[int], np.ndarray],
    image_info: ImageMetadata,
    maxadu: int,
    hdr: fits.Header,
    device_name: str,
    dateobs: datetime,
    folder: str,
    wcs: Optional[WCS] = None,
) -> Path:
    """
    Save an image to disk in FITS format with proper headers and filename generation.

    This function transforms raw image data, updates FITS headers with observation
    metadata, optionally adds WCS information, and saves the result as a FITS file
    with an automatically generated filename based on image properties.

    Parameters
    ----------
    image : list of int or np.ndarray
        Raw image data to be saved
    image_info : ImageMetadata
        Metadata object containing image properties for data type determination
    maxadu : int
        Maximum ADU (Analog-to-Digital Unit) value for the image
    hdr : fits.Header
        FITS header dictionary containing image metadata. Must include:
        - FILTER: Filter name used for the observation
        - IMAGETYP: Type of image ("Light Frame", "Bias Frame", "Dark Frame", etc.)
        - OBJECT: Target object name (for light frames)
        - EXPTIME: Exposure time in seconds
    device_name : str
        Name of the camera/device used for the observation
    dateobs : datetime
        UTC datetime when the exposure started
    folder : str
        Subfolder name within the images directory where the file will be saved
    wcs : WCS, optional
        World Coordinate System information to add to the header, by default None

    Returns
    -------
    Path
        Path object pointing to the saved FITS file

    Notes
    -----
    The function automatically generates filenames based on image type:
    - Light frames: "{device}_{filter}_{object}_{exptime}_{timestamp}.fits"
    - Bias/Dark frames: "{device}_{imagetype}_{exptime}_{timestamp}.fits"
    - Other frames: "{device}_{filter}_{imagetype}_{exptime}_{timestamp}.fits"
    
    The FITS header is automatically updated with:
    - DATE-OBS: UTC date/time of exposure start
    - DATE: UTC date/time when the file was written
    - WCS information (if provided)
    """

    # transform image to numpy array
    image_array = transform_image_to_array(
        image, maxadu=maxadu, image_info=image_info
    )  ## TODO: make more efficient?

    # update FITS header
    hdr["DATE-OBS"] = (
        dateobs.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time of exposure start",
    )

    date = datetime.now(UTC)
    hdr["DATE"] = (
        date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time when this file was written",
    )

    # add WCS information
    if wcs:
        hdr.extend(wcs.to_header(), update=True)

    # create FITS HDU
    hdu = fits.PrimaryHDU(image_array, header=hdr)

    # create filename
    filter_name = hdr["FILTER"].replace("'", "")
    if hdr["IMAGETYP"] == "Light Frame":
        filename = f"{device_name}_{filter_name}_{hdr['OBJECT']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    elif hdr["IMAGETYP"] in ["Bias Frame", "Dark Frame"]:
        filename = f"{device_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    else:
        filename = f"{device_name}_{filter_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"

    filepath = CONFIG.paths.images / folder / filename

    # save FITS file
    hdu.writeto(filepath)

    return filepath
