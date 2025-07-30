import hashlib
import math
from typing import List, Set


class BloomFilter:
    """
    Bloom filter for efficient duplicate detection of CSV rows.
    """
    
    def __init__(self, expected_elements: int = 1000000, false_positive_rate: float = 0.01):
        """
        Initialize Bloom filter.
        
        Args:
            expected_elements: Expected number of elements
            false_positive_rate: Desired false positive rate
        """
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        
        # Calculate optimal size and number of hash functions
        self.size = self._calculate_size(expected_elements, false_positive_rate)
        self.num_hashes = self._calculate_num_hashes(self.size, expected_elements)
        
        # Initialize bit array
        self.bit_array = [0] * self.size
        self.count = 0
    
    def _calculate_size(self, n: int, p: float) -> int:
        """Calculate optimal size for given n elements and false positive rate p."""
        return int(-n * math.log(p) / (math.log(2) ** 2))
    
    def _calculate_num_hashes(self, m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        return int(m / n * math.log(2))
    
    def _get_hash_positions(self, item: str) -> List[int]:
        """Get hash positions for an item."""
        positions = []
        for i in range(self.num_hashes):
            # Create different hash by appending index
            hash_input = f"{item}:{i}".encode()
            hash_value = int(hashlib.sha256(hash_input).hexdigest(), 16)
            position = hash_value % self.size
            positions.append(position)
        return positions
    
    def add(self, item: str) -> bool:
        """
        Add item to Bloom filter.
        
        Args:
            item: String to add
            
        Returns:
            True if item was already present (duplicate)
        """
        positions = self._get_hash_positions(item)
        
        # Check if all positions are already set (likely duplicate)
        is_duplicate = all(self.bit_array[pos] == 1 for pos in positions)
        
        # Set all positions
        for pos in positions:
            self.bit_array[pos] = 1
        
        if not is_duplicate:
            self.count += 1
        
        return is_duplicate
    
    def contains(self, item: str) -> bool:
        """
        Check if item is in Bloom filter.
        
        Args:
            item: String to check
            
        Returns:
            True if item is likely present
        """
        positions = self._get_hash_positions(item)
        return all(self.bit_array[pos] == 1 for pos in positions)
    
    def get_stats(self) -> dict:
        """Get Bloom filter statistics."""
        return {
            "size": self.size,
            "num_hashes": self.num_hashes,
            "count": self.count,
            "expected_elements": self.expected_elements,
            "false_positive_rate": self.false_positive_rate,
            "load_factor": self.count / self.expected_elements
        }


def hash_csv_row(row_data: str) -> str:
    """
    Hash a CSV row for duplicate detection.
    
    Args:
        row_data: CSV row as string
        
    Returns:
        SHA256 hash of the row
    """
    return hashlib.sha256(row_data.encode('utf-8')).hexdigest()


def detect_new_rows(hashes: List[str], bloom_filter: BloomFilter) -> dict:
    """
    Detect new rows using Bloom filter.
    
    Args:
        hashes: List of row hashes to check
        bloom_filter: Bloom filter instance
        
    Returns:
        Dict with new row count and duplicate count
    """
    new_count = 0
    duplicate_count = 0
    
    for row_hash in hashes:
        if bloom_filter.add(row_hash):
            duplicate_count += 1
        else:
            new_count += 1
    
    return {
        "new_rows": new_count,
        "duplicate_rows": duplicate_count,
        "total_rows": len(hashes),
        "uniqueness_ratio": new_count / len(hashes) if hashes else 0.0
    } 