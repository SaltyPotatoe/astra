from datetime import UTC, datetime

import numpy as np
from alpaca.camera import ImageMetadata
from astropy.io import fits

from astra import CONFIG


def img_transform(img: np.array, maxadu: int, imginfo: ImageMetadata) -> np.array:
    """
    This function takes in a device object, an image object, and a maximum ADU
    value and returns a numpy array of the correct shape for astropy.io.fits.

    Parameters:
        device (AlpacaDevice): A device object that contains the ImageArrayInfo data.
        img (np.array): An image object that contains the image data.
        maxadu (int): The maximum ADU value.

    Returns:
        nda (np.array): A numpy array of the correct shape for astropy.io.fits.
    """

    # Determine the image data type
    if imginfo.ImageElementType == 0 or imginfo.ImageElementType == 1:
        imgDataType = np.uint16
    elif imginfo.ImageElementType == 2:
        if maxadu <= 65535:
            imgDataType = np.uint16  # Required for BZERO & BSCALE to be written
        else:
            imgDataType = np.int32
    elif imginfo.ImageElementType == 3:
        imgDataType = np.float64
    else:
        raise ValueError(f"Unknown ImageElementType: {imginfo.ImageElementType}")

    # Make a numpy array of the correct shape for astropy.io.fits
    if imginfo.Rank == 2:
        nda = np.array(img, dtype=imgDataType).transpose()
    else:
        nda = np.array(img, dtype=imgDataType).transpose(2, 1, 0)

    return nda


def save_image(
    image_array: list[int],
    imginfo: ImageMetadata,
    maxadu: int,
    hdr: fits.Header,
    device_name: str,
    dateobs: datetime,
    folder: str,
) -> str:
    """
    Save an image to disk.

    This function retrieves an image from an Alpaca device, transforms it, and saves it to disk in FITS format.
    The filename is generated based on device information and the image's characteristics.

    The FITS header is updated with the 'DATE-OBS' and 'DATE' keywords to record the exposure start time
    and the time when the file was written.

    After saving the image, it is logged, and its file path is returned.

    Parameters:

    Returns:
        str: The file path to the saved image.

    """

    # transform image to numpy array
    img = np.array(image_array)
    nda = img_transform(img, maxadu, imginfo)  ## TODO: make more efficient?

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

    # create FITS HDU
    hdu = fits.PrimaryHDU(nda, header=hdr)

    # create filename
    filter_name = hdr["FILTER"].replace("'", "")
    if hdr["IMAGETYP"] == "Light Frame":
        filename = f"{device_name}_{filter_name}_{hdr['OBJECT']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    elif hdr["IMAGETYP"] in ["Bias Frame", "Dark Frame"]:
        filename = f"{device_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    else:
        filename = f"{device_name}_{filter_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"

    filepath = CONFIG.folder_images / folder / filename

    # save FITS file
    hdu.writeto(filepath)

    return filepath
