"""
Login/ANON_REQ packet handling helper for pyMC Repeater.

This module processes login requests and manages authentication for all identities.
"""

import asyncio
import logging

from pymc_core.node.handlers.login_server import LoginServerHandler

logger = logging.getLogger("LoginHelper")


class LoginHelper:
    """Helper class for processing ANON_REQ login packets in the repeater."""

    def __init__(self, identity_manager, packet_injector=None, acl=None, log_fn=None):

        self.identity_manager = identity_manager
        self.packet_injector = packet_injector
        self.log_fn = log_fn or logger.info
        self.acl = acl
        
        self.handlers = {}

    def register_identity(self, name: str, identity, identity_type: str = "room_server"):

        if not self.acl:
            logger.warning(f"Cannot register identity '{name}': no ACL configured")
            return
        
        handler = LoginServerHandler(
            local_identity=identity,
            log_fn=self.log_fn,
            authenticate_callback=self.acl.authenticate_client,
        )
        
        handler.set_send_packet_callback(self._send_packet_with_delay)
        
        hash_byte = identity.get_public_key()[0]
        self.handlers[hash_byte] = handler
        
        logger.info(f"Registered {identity_type} '{name}' login handler: hash=0x{hash_byte:02X}")

    async def process_login_packet(self, packet):

        try:
            if len(packet.payload) < 1:
                return False
            
            dest_hash = packet.payload[0]
            
            handler = self.handlers.get(dest_hash)
            if handler:
                logger.debug(f"Routing login to identity: hash=0x{dest_hash:02X}")
                await handler(packet)
                packet.mark_do_not_retransmit()
                return True
            else:
                logger.debug(f"No login handler registered for hash 0x{dest_hash:02X}, allowing forward")
                return False
                
        except Exception as e:
            logger.error(f"Error processing login packet: {e}")
            return False

    def _send_packet_with_delay(self, packet, delay_ms: int):
   
        if self.packet_injector:
            asyncio.create_task(self._delayed_send(packet, delay_ms))
        else:
            logger.error("No packet injector configured, cannot send login response")

    async def _delayed_send(self, packet, delay_ms: int):
 
        await asyncio.sleep(delay_ms / 1000.0)
        try:
            await self.packet_injector(packet, wait_for_ack=False)
            logger.debug(f"Sent login response after {delay_ms}ms delay")
        except Exception as e:
            logger.error(f"Error sending login response: {e}")

    def get_acl(self):
        return self.acl

    def list_authenticated_clients(self):
        if self.acl:
            return self.acl.get_all_clients()
        return []
