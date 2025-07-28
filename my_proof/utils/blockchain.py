import json
import os
from web3 import Web3
import logging

from my_proof.config import settings

class BlockchainClient:
    """Client for interacting with blockchain contracts."""
    
    def __init__(self):
        """Initialize the blockchain client using global settings."""
        try:
            self.w3 = Web3(Web3.HTTPProvider(settings.RPC_URL))
            
            # Load the contract ABI
            contract_path = os.path.join(os.path.dirname(__file__), '..', 'contracts', 'dlp-contract.json')
            with open(contract_path, 'r') as f:
                contract_abi = json.load(f)
                
            # Create contract instance
            self.contract = self.w3.eth.contract(
                address=settings.DLP_CONTRACT_ADDRESS,
                abi=contract_abi
            )
            
        except Exception as e:
            logging.error(f"Failed to initialize blockchain client: {str(e)}")
            raise

    def get_contributor_file_count(self) -> int:
        """
        Get the number of files contributed by the configured address.
        
        Returns:
            int: Number of files contributed by the address
        """
        try:
            if not settings.OWNER_ADDRESS:
                raise ValueError("OWNER_ADDRESS is not set in environment")
            
            contributor_info = self.contract.functions.contributorInfo(
                Web3.to_checksum_address(settings.OWNER_ADDRESS)
            ).call()
            
            return contributor_info[1]  # [contributorAddress, filesListCount]
            
        except Exception as e:
            logging.error(f"Error getting contributor file count: {str(e)}")
            return 0
