"""
Text message (TXT_MSG) handling helper for pyMC Repeater.

This module processes incoming text messages for all managed identities
(repeater identity + identity manager identities).
"""

import asyncio
import logging

from pymc_core.node.handlers.text import TextMessageHandler

logger = logging.getLogger("TextHelper")


class TextHelper:

    def __init__(self, identity_manager, packet_injector=None, acl=None, log_fn=None):

        self.identity_manager = identity_manager
        self.packet_injector = packet_injector
        self.log_fn = log_fn or logger.info
        self.acl = acl
        
        # Dictionary of handlers keyed by dest_hash
        self.handlers = {}

    def register_identity(
        self, 
        name: str, 
        identity, 
        identity_type: str = "room_server",
        radio_config=None
    ):

        if not self.acl:
            logger.warning(f"Cannot register identity '{name}': no ACL configured")
            return
        
        # Create a contacts wrapper from ACL
        acl_contacts = self._create_acl_contacts_wrapper()
        
        # Create TextMessageHandler for this identity
        handler = TextMessageHandler(
            local_identity=identity,
            contacts=acl_contacts,
            log_fn=self.log_fn,
            send_packet_fn=self._send_packet,
            radio_config=radio_config,
        )
        
        # Register by dest hash
        hash_byte = identity.get_public_key()[0]
        self.handlers[hash_byte] = {
            "handler": handler,
            "identity": identity,
            "name": name,
            "type": identity_type,
        }
        
        logger.info(
            f"Registered {identity_type} '{name}' text handler: hash=0x{hash_byte:02X}"
        )
    
    def _create_acl_contacts_wrapper(self):

        class ACLContactsWrapper:
            def __init__(self, acl):
                self._acl = acl
            
            @property
            def contacts(self):
                contact_list = []
                for client_info in self._acl.get_all_clients():
                    # Create a minimal contact object that TextMessageHandler needs
                    class ContactProxy:
                        def __init__(self, client):
                            self.public_key = client.id.get_public_key().hex()
                            self.name = f"client_{self.public_key[:8]}"
                    
                    contact_list.append(ContactProxy(client_info))
                return contact_list
        
        return ACLContactsWrapper(self.acl)

    async def process_text_packet(self, packet):

        try:
            if len(packet.payload) < 2:
                return False
            
            dest_hash = packet.payload[0]
            src_hash = packet.payload[1]
            
            handler_info = self.handlers.get(dest_hash)
            if handler_info:
                logger.debug(
                    f"Routing text message to '{handler_info['name']}': "
                    f"dest=0x{dest_hash:02X}, src=0x{src_hash:02X}"
                )
                
                # Call the handler
                await handler_info["handler"](packet)
                
                # Call placeholder for custom processing
                await self._on_message_received(
                    identity_name=handler_info["name"],
                    identity_type=handler_info["type"],
                    packet=packet,
                    dest_hash=dest_hash,
                    src_hash=src_hash,
                )
                
                # Mark packet as handled
                packet.mark_do_not_retransmit()
                return True
            else:
                logger.debug(
                    f"No text handler for hash 0x{dest_hash:02X}, allowing forward"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error processing text packet: {e}")
            return False

    async def _on_message_received(
        self,
        identity_name: str,
        identity_type: str,
        packet,
        dest_hash: int,
        src_hash: int,
    ):

        # Placeholder - can be overridden or callback can be added
        logger.debug(
            f"Message received for {identity_type} '{identity_name}' "
            f"from 0x{src_hash:02X}"
        )
        
        # Example: Extract decrypted message if available
        if hasattr(packet, "decrypted") and packet.decrypted:
            message_text = packet.decrypted.get("text", "<unknown>")
            logger.info(
                f"[{identity_type}:{identity_name}] Message: {message_text}"
            )

    async def _send_packet(self, packet, wait_for_ack: bool = False):

        if self.packet_injector:
            try:
                return await self.packet_injector(packet, wait_for_ack=wait_for_ack)
            except Exception as e:
                logger.error(f"Error sending packet: {e}")
                return False
        else:
            logger.error("No packet injector configured, cannot send packet")
            return False

    def set_message_callback(self, callback):

        self._message_callback = callback

    def list_registered_identities(self):

        return [
            {
                "hash": hash_byte,
                "name": info["name"],
                "type": info["type"],
            }
            for hash_byte, info in self.handlers.items()
        ]
