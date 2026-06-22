"""CFAR vessel detector for Sentinel-1 VV imagery."""

import numpy as np
from scipy.ndimage import uniform_filter, label
from scipy.special import erfinv


def cfar_detector(image_db, guard=5, background=15, pfa=1e-4):
    """CFAR detector on a dB SAR image. Returns (detection mask, threshold surface).

    Each pixel is compared to the mean and std of a ring around it (the background
    window minus a guard window), and flagged if it exceeds the threshold set by pfa.
    """
    guard_size = 2 * guard + 1
    bg_size = 2 * (guard + background) + 1

    img = image_db.astype(np.float64)
    bg_mean = uniform_filter(img, size=bg_size)
    bg_sq_mean = uniform_filter(img ** 2, size=bg_size)
    guard_mean = uniform_filter(img, size=guard_size)
    guard_sq_mean = uniform_filter(img ** 2, size=guard_size)

    n_bg = bg_size ** 2
    n_guard = guard_size ** 2
    n_ring = n_bg - n_guard

    ring_mean = (bg_mean * n_bg - guard_mean * n_guard) / n_ring
    ring_sq = (bg_sq_mean * n_bg - guard_sq_mean * n_guard) / n_ring
    ring_std = np.sqrt(np.maximum(ring_sq - ring_mean ** 2, 1e-10))

    threshold = ring_mean + np.sqrt(2) * erfinv(1 - 2 * pfa) * ring_std
    return image_db > threshold, threshold


def to_db(arr):
    arr = arr.astype(np.float32)
    return np.where(arr > 0, 10 * np.log10(arr), np.nan)


def detect(image_db, water_mask=None, pfa=1e-4, guard=5, background=15,
           min_pixels=3, max_pixels=500):
    """Full CFAR detection on a dB chip.

    The whole detector in one call: CFAR threshold, drop NaNs and land, then a
    size filter. Returns a list of (row, col, size_pixels), one per vessel
    candidate. This is what the registered CFAR model wraps; all the knobs that
    move the false-alarm rate (pfa and the size bounds) are arguments here.
    """
    filled = np.nan_to_num(image_db, nan=np.nanmedian(image_db))
    mask, _ = cfar_detector(filled, guard=guard, background=background, pfa=pfa)
    mask = mask & ~np.isnan(image_db)

    if water_mask is not None:
        from skimage.transform import resize
        wm = resize(water_mask.astype(float), image_db.shape, order=0,
                    preserve_range=True) > 0.5
        mask = mask & wm

    labeled, n = label(mask)
    if n == 0:
        return []
    sizes = np.bincount(labeled.ravel())
    dets = []
    for i in range(1, n + 1):
        size = int(sizes[i])
        if min_pixels <= size <= max_pixels:
            ys, xs = np.where(labeled == i)
            dets.append((float(ys.mean()), float(xs.mean()), size))
    return dets
