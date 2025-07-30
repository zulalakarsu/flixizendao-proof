import hashlib
import hmac
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def client_side_decrypt(encrypted_data: bytes, signature: str) -> bytes:
    """
    Decrypt data that was encrypted with client-side encryption.
    
    Args:
        encrypted_data: The encrypted data blob
        signature: The wallet signature used for encryption
        
    Returns:
        The decrypted data
    """
    try:
        # Extract encryption parameters from the signature
        # This assumes the same format as the client-side encryption
        key_material = hashlib.sha256(signature.encode()).digest()
        
        # For AES-GCM, we need to extract IV and ciphertext
        # This is a simplified version - in practice you'd need to match
        # the exact client-side encryption format
        if len(encrypted_data) < 32:  # Minimum size for IV + some data
            raise ValueError("Encrypted data too short")
        
        # Extract IV (first 12 bytes for AES-GCM)
        iv = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(key_material),
            modes.GCM(iv),
            backend=default_backend()
        )
        
        # Decrypt
        decryptor = cipher.decryptor()
        decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()
        
        return decrypted_data
        
    except Exception as e:
        raise Exception(f"Decryption failed: {e}")


def verify_signature(data: bytes, signature: str) -> bool:
    """
    Verify that the signature matches the data.
    
    Args:
        data: The data to verify
        signature: The signature to verify against
        
    Returns:
        True if signature is valid
    """
    try:
        # This is a simplified verification
        # In practice, you'd verify against the actual wallet signature
        expected_hash = hashlib.sha256(data).hexdigest()
        return signature == expected_hash
    except:
        return False 