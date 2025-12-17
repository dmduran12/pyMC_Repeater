"""
Text message (TXT_MSG) handling helper for pyMC Repeater.

This module processes incoming text messages for all managed identities
(repeater identity + identity manager identities).
Also handles CLI commands for admin users on the repeater identity.
"""

import asyncio
import logging

from pymc_core.node.handlers.text import TextMessageHandler
from .repeater_cli import RepeaterCLI

logger = logging.getLogger("TextHelper")


class TextHelper:

    def __init__(self, identity_manager, packet_injector=None, acl_dict=None, log_fn=None, 
                 config_path: str = None, config: dict = None, save_config_callback=None):

        self.identity_manager = identity_manager
        self.packet_injector = packet_injector
        self.log_fn = log_fn or logger.info
        self.acl_dict = acl_dict or {}  # Per-identity ACLs keyed by hash_byte
        
        # Dictionary of handlers keyed by dest_hash
        self.handlers = {}
        
        # Track repeater identity for CLI commands
        self.repeater_hash = None
        
        # Initialize CLI handler if config provided
        self.cli = None
        if config_path and config and save_config_callback:
            self.cli = RepeaterCLI(config_path, config, save_config_callback)
            logger.info("Initialized CLI handler for repeater commands")

    def register_identity(
        self, 
        name: str, 
        identity, 
        identity_type: str = "room_server",
        radio_config=None
    ):

        hash_byte = identity.get_public_key()[0]
        
        # Get ACL for this identity
        identity_acl = self.acl_dict.get(hash_byte)
        if not identity_acl:
            logger.warning(f"Cannot register identity '{name}': no ACL for hash 0x{hash_byte:02X}")
            return
        
        # Create a contacts wrapper from this identity's ACL
        acl_contacts = self._create_acl_contacts_wrapper(identity_acl)
        
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
        
        # Track repeater identity for CLI commands
        if identity_type == "repeater":
            self.repeater_hash = hash_byte
            logger.info(f"Set repeater hash for CLI: 0x{hash_byte:02X}")
        
        logger.info(
            f"Registered {identity_type} '{name}' text handler: hash=0x{hash_byte:02X}"
        )
    
    def _create_acl_contacts_wrapper(self, acl):

        class ACLContactsWrapper:
            def __init__(self, identity_acl):
                self._acl = identity_acl
            
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
        
        return ACLContactsWrapper(acl)

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
                
                # Let handler decrypt the message first
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
        
        # Extract decrypted message if available
        if hasattr(packet, "decrypted") and packet.decrypted:
            message_text = packet.decrypted.get("text", "<unknown>")
            
            # Clean message text - remove null bytes and trailing whitespace
            message_text = message_text.rstrip('\x00').rstrip()
            
            logger.info(
                f"[{identity_type}:{identity_name}] Message: {message_text}"
            )
            
            # Check if this is a CLI command to the repeater (AFTER decryption)
            if dest_hash == self.repeater_hash and self.cli and self._is_cli_command(message_text):
                try:
                    # Check admin permission
                    is_admin = self._check_admin_permission(src_hash)
                    
                    # If not admin, log and return without sending reply
                    if not is_admin:
                        logger.warning(f"CLI command denied from 0x{src_hash:02X} (not admin): {message_text[:50]}")
                        return
                    
                    # Get client for full public key
                    repeater_acl = self.acl_dict.get(self.repeater_hash)
                    sender_pubkey = bytes([src_hash]) + b'\x00' * 31  # Default
                    if repeater_acl:
                        for client_info in repeater_acl.get_all_clients():
                            if client_info.id.get_public_key()[0] == src_hash:
                                sender_pubkey = client_info.id.get_public_key()
                                break
                    
                    # Handle CLI command
                    reply = self.cli.handle_command(
                        sender_pubkey=sender_pubkey,
                        command=message_text,
                        is_admin=is_admin
                    )
                    
                    logger.info(f"CLI command from 0x{src_hash:02X}: {message_text[:50]} -> {reply[:100]}")
                    
                    # Send reply back to sender
                    handler_info = self.handlers.get(dest_hash)
                    if handler_info:
                        await self._send_cli_reply(packet, reply, handler_info)
                    
                except Exception as e:
                    logger.error(f"Error processing CLI command: {e}", exc_info=True)

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
    
    def _is_cli_command(self, message: str) -> bool:
        """Check if message looks like a CLI command."""
        # Strip optional sequence prefix (XX|)
        if len(message) > 4 and message[2] == '|':
            message = message[3:].strip()
        
        # Check for known command prefixes
        command_prefixes = [
            "get ", "set ", "reboot", "advert", "clock", "time ",
            "password ", "clear ", "ver", "board", "neighbors", "neighbor.",
            "tempradio ", "setperm ", "region", "sensor ", "gps", "log ",
            "stats-", "start ota"
        ]
        
        return any(message.startswith(prefix) for prefix in command_prefixes)
    
    def _check_admin_permission(self, src_hash: int) -> bool:
        """Check if sender has admin permissions (bit 0x02)."""
        # Get the repeater's ACL
        repeater_acl = self.acl_dict.get(self.repeater_hash)
        if not repeater_acl:
            return False
        
        # Get client by hash byte
        clients = repeater_acl.get_all_clients()
        for client_info in clients:
            pubkey = client_info.id.get_public_key()
            if pubkey[0] == src_hash:
                # Check admin bit (0x02 = PERM_ACL_ADMIN)
                permissions = getattr(client_info, 'permissions', 0)
                PERM_ACL_ADMIN = 0x02
                return (permissions & 0x02) == PERM_ACL_ADMIN
        
        return False
    
    async def _send_cli_reply(self, original_packet, reply_text: str, handler_info: dict):
        """
        Send CLI reply back to sender using TXT_MSG datagram.
        
        Follows the C++ pattern (lines 603-609 in MyMesh.cpp):
        - Creates TXT_MSG datagram with TXT_TYPE_CLI_DATA flag
        - Encrypts with shared secret from ACL client
        - Uses client->out_path_len to decide routing:
          * if out_path_len < 0: sendFlood()
          * else: sendDirect() with stored out_path
        """
        from pymc_core.protocol import PacketBuilder, Identity
        from pymc_core.protocol.constants import PAYLOAD_TYPE_TXT_MSG
        import time
        
        try:
            src_hash = original_packet.payload[1]
            dest_hash = original_packet.payload[0]
            
            incoming_route = original_packet.get_route_type()
            logger.debug(f"CLI reply: original packet dest=0x{dest_hash:02X}, src=0x{src_hash:02X}, incoming_route={incoming_route}")
            
            # Find the client in repeater's ACL to get shared secret
            repeater_acl = self.acl_dict.get(self.repeater_hash)
            if not repeater_acl:
                logger.error("No repeater ACL found for CLI reply")
                return
            
            client = None
            for client_info in repeater_acl.get_all_clients():
                pubkey = client_info.id.get_public_key()
                if pubkey[0] == src_hash:
                    client = client_info
                    break
            
            if not client:
                logger.error(f"Client 0x{src_hash:02X} not found in ACL for CLI reply")
                return
            
            # Get shared secret from client
            shared_secret = client.shared_secret
            if not shared_secret or len(shared_secret) == 0:
                logger.error(f"No shared secret for client 0x{src_hash:02X}")
                return
            
            # Build reply packet payload
            # Format: timestamp(4) + flags(1) + reply_text
            timestamp = int(time.time())
            TXT_TYPE_CLI_DATA = 0x01
            flags = (TXT_TYPE_CLI_DATA << 2)  # Upper 6 bits are txt_type
            
            reply_bytes = reply_text.encode('utf-8')
            plaintext = timestamp.to_bytes(4, 'little') + bytes([flags]) + reply_bytes
            
            # Decide routing based on client->out_path_len (C++ pattern)
            # out_path is populated by PATH packets, NOT from incoming text message route
            route_type = "flood" if client.out_path_len < 0 else "direct"
            logger.debug(f"CLI reply: client.out_path_len={client.out_path_len}, using route_type={route_type}")
            
            reply_packet = PacketBuilder.create_datagram(
                ptype=PAYLOAD_TYPE_TXT_MSG,
                dest=client.id,
                local_identity=handler_info["identity"],
                secret=shared_secret,
                plaintext=plaintext,
                route_type=route_type
            )
            
            # Debug reply packet structure
            if len(reply_packet.payload) >= 2:
                reply_dest_hash = reply_packet.payload[0]
                reply_src_hash = reply_packet.payload[1]
                logger.debug(f"CLI reply: Packet created - dest=0x{reply_dest_hash:02X}, src=0x{reply_src_hash:02X}, route={reply_packet.get_route_type()}")
            
            # Add path for direct routing if available from PATH packets
            if client.out_path_len >= 0 and len(client.out_path) > 0:
                reply_packet.path = bytearray(client.out_path[:client.out_path_len])
                reply_packet.path_len = client.out_path_len
                logger.debug(f"CLI reply: Added stored out_path - path_len={reply_packet.path_len}, path={[hex(b) for b in reply_packet.path]}")
            
            # Send with delay (CLI_REPLY_DELAY_MILLIS = 600ms in C++)
            CLI_REPLY_DELAY_MS = 600
            await asyncio.sleep(CLI_REPLY_DELAY_MS / 1000.0)
            
            await self._send_packet(reply_packet, wait_for_ack=False)
            logger.info(f"CLI reply sent to 0x{src_hash:02X} via {route_type.upper()}: {reply_text[:50]}")
            
        except Exception as e:
            logger.error(f"Error sending CLI reply: {e}", exc_info=True)
