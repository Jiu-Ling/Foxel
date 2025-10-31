"""
Utility functions shared across storage adapters.
"""
from typing import List, Dict, Tuple
import asyncio
from pathlib import Path


def sort_and_paginate_entries(
    entries: List[Dict],
    page_num: int = 1,
    page_size: int = 50,
    sort_by: str = "name",
    sort_order: str = "asc"
) -> Tuple[List[Dict], int]:
    """
    Sort and paginate a list of file/directory entries.
    
    Args:
        entries: List of entry dictionaries with keys like 'name', 'is_dir', 'size', 'mtime'
        page_num: Page number (1-indexed)
        page_size: Number of entries per page
        sort_by: Field to sort by ('name', 'size', 'mtime')
        sort_order: Sort order ('asc' or 'desc')
    
    Returns:
        Tuple of (paginated entries, total count)
    """
    # Sort entries
    reverse = sort_order.lower() == "desc"
    
    def get_sort_key(item):
        # Directories first, then sort by specified field
        key = (not item["is_dir"],)
        sort_field = sort_by.lower()
        
        if sort_field == "name":
            key += (item["name"].lower(),)
        elif sort_field == "size":
            key += (item["size"],)
        elif sort_field == "mtime":
            key += (item["mtime"],)
        else:  # Default to name
            key += (item["name"].lower(),)
        return key
    
    entries.sort(key=get_sort_key, reverse=reverse)
    
    total_count = len(entries)
    
    # Paginate
    start_idx = (page_num - 1) * page_size
    end_idx = start_idx + page_size
    page_entries = entries[start_idx:end_idx]
    
    return page_entries, total_count


def parse_range_header(range_header: str | None, file_size: int) -> Tuple[int, int, int, bool]:
    """
    Parse HTTP Range header and return start, end, status code, and whether partial content.
    
    Args:
        range_header: HTTP Range header value (e.g., "bytes=0-1023")
        file_size: Total size of the file
    
    Returns:
        Tuple of (start_byte, end_byte, status_code, is_partial)
        
    Raises:
        ValueError: If range header is invalid
        IndexError: If range is not satisfiable
    """
    start = 0
    end = file_size - 1
    status = 200
    is_partial = False
    
    if range_header and range_header.startswith("bytes="):
        part = range_header.removeprefix("bytes=")
        s, e = part.split("-", 1)
        
        if s.strip():
            start = int(s)
        if e.strip():
            end = int(e)
        
        if start >= file_size:
            raise IndexError("Requested Range Not Satisfiable")
        
        if end >= file_size:
            end = file_size - 1
        
        status = 206
        is_partial = True
    
    return start, end, status, is_partial


def build_stream_headers(
    content_type: str,
    file_size: int,
    start: int,
    end: int,
    is_partial: bool,
    filename: str | None = None
) -> Dict[str, str]:
    """
    Build HTTP headers for file streaming response.
    
    Args:
        content_type: MIME type of the content
        file_size: Total size of the file
        start: Start byte position
        end: End byte position
        is_partial: Whether this is a partial content response (206)
        filename: Optional filename for Content-Disposition header
    
    Returns:
        Dictionary of HTTP headers
    """
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
    }
    
    if is_partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(end - start + 1)
    else:
        headers["Content-Length"] = str(file_size)
    
    if filename:
        from urllib.parse import quote
        headers["Content-Disposition"] = f'inline; filename="{quote(filename)}"'
    
    return headers


async def extract_exif_data(file_path: Path) -> Dict[str, str] | None:
    """
    Extract EXIF data from an image file.
    
    Args:
        file_path: Path to the image file
    
    Returns:
        Dictionary of EXIF data or None if not available
    """
    try:
        from PIL import Image
        img = await asyncio.to_thread(Image.open, file_path)
        exif_data = img._getexif()
        if exif_data:
            return {str(k): str(v) for k, v in exif_data.items()}
    except Exception:
        pass
    return None
