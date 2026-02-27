import pytest
import re

@pytest.fixture
def regex():
    return r"(?P<region>[^:]+):(?P<start>\d+)-(?P<end>\d+):?(?P<strand>[+-])?"

def generate_location(region, start, end, strand=None):
    strand_part = f":{strand}" if strand else ""
    return f"{region}:{start}-{end}{strand_part}"

@pytest.mark.parametrize("region, start, end, strand", [
    ("chr1", "12345", "67890", "+"),
    ("chr1", "12345", "67890", "-"),
    ("chr1", "12345", "67890", None),
])
def test_location_regex(regex, region, start, end, strand):
    location = generate_location(region, start, end, strand)
    match = re.match(regex, location)
    
    assert match is not None
    assert match.group("region") == region
    assert match.group("start") == start
    assert match.group("end") == end
    assert match.group("strand") == strand
