import json
import telethon.sync

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)

class Telegram:

    def __init__(self, telegram_apis):
        self.telegram_apis = telegram_apis
        self.api =  self.__authenticate(self.telegram_apis)

    def get_channel(self, channel):
        client = self.api

        if channel.isdigit():
            entity = PeerChannel(int(channel))
        else:
            entity = channel

        my_channel = client.get_entity(entity)
        offset = 0
        limit = 100
        all_participants = []

        while True:
            participants = client(GetParticipantsRequest(
                my_channel, ChannelParticipantsSearch(''), offset, limit,
                hash=0
            ))
            if not participants.users:
                break
            all_participants.extend(participants.users)
            offset += len(participants.users)

        all_user_details = []
        for participant in all_participants:
            all_user_details.append(
                {
                    "id": participant.id,
                    "first_name": participant.first_name,
                    "last_name": participant.last_name,
                    "user": participant.username,
                    "phone": participant.phone,
                    "is_bot": participant.bot,
                    "contact": participant.contact,
                    "mutual_contact": participant.mutual_contact,
	            "scam": participant.scam
                }
            )

        return all_user_details

    def get_messages(self, channel):
        client = self.api

        if channel.isdigit():
            entity = PeerChannel(int(channel))
        else:
            entity = channel

        my_channel = client.get_entity(entity)

        offset_id = 0
        limit = 100
        all_messages = []
        total_messages = 0
        total_count_limit = 0

        while True:
            print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
            history = client(GetHistoryRequest(
                peer=my_channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                all_messages.append(message.to_dict())
            offset_id = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break
        return all_messages

    def __authenticate(self, credentials):
        # Setting configuration values
        api_id = credentials['app_id']
        api_hash = credentials['api_hash']

        api_hash = str(api_hash)

        phone = credentials['phone']
        username = credentials['username']
        client = TelegramClient(username, api_id, api_hash)
        client.start()
        print("Client Created")
        # Ensure you're authorized
        if not client.is_user_authorized():
            client.send_code_request(phone)
            try:
                client.sign_in(phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                client.sign_in(password=input('Password: '))

        return client
