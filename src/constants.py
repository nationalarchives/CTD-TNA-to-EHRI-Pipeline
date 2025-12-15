"""
module for defining constants and constant namespaces.
"""

from pathlib import Path

class DataFolders():
    __slots__ = ()

    _root = Path(r"C:\Users\rbruno\OneDrive - The National Archives\Projects\EHRI\Data\Pipeline")
    INPUT = _root / "1-INPUT"
    CACHE = _root / "2-CACHE/TNA_records_lineage.db"
    TRANSFORM = _root / "3-TRANSFORM"
    ARCHIVE = _root / "4-ARCHIVE"


DATA = DataFolders()
DISCOVERY_API_URI = r"https://discovery.nationalarchives.gov.uk/API"
PAUSE_IN_SECONDS = 2
PAGE_SIZE = 1000

