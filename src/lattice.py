from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Optional

from anduril.entitymanager.v1.entity_manager_api.pub_pb2_grpc import EntityManagerAPIStub

from anduril.entitymanager.v1.entity_manager_api.pub_pb2 import (
    GetEntityRequest,
    GetEntityResponse,
    PublishEntityRequest,
    PublishEntityResponse,
)

from anduril.entitymanager.v1.entity.pub_pb2 import(
    Aliases,
    AlternateId,
    Entity, 
    Provenance,
)
from anduril.entitymanager.v1.classification.pub_pb2 import(
    Classification,
    ClassificationInformation,
    ClassificationLevels,
)

from anduril.entitymanager.v1.location.pub_pb2 import (
    Location,
    Position,
)
from anduril.entitymanager.v1.types.pub_pb2 import (
    AltIdType,
    Template,
)
from anduril.entitymanager.v1.ontology.pub_pb2 import(
    MilView,
    Ontology,
)

from anduril.ontology.v1.type.pub_pb2 import (
    Disposition,
    Environment,
)

import grpc
import requests
import time

from ais import VesselData

EXPIRY_OFFSET_SECONDS = 10
PORT = 443

class Lattice:
    def __init__(self, logger: Logger, lattice_endpoint: str, sandboxes_token: str, client_id: str, client_secret: str):       
        self.logger = logger
        self.lattice_endpoint = lattice_endpoint
        self.port = PORT
        self.client_id = client_id
        self.client_secret = client_secret

        if sandboxes_token:
            self.sandboxes_token = sandboxes_token
            self.generated_metadata = (("anduril-sandbox-authorization", f"Bearer {sandboxes_token}"),)
        
        self.token_expiry_time = 0
        self.auth_token = ""
        self.refresh_token()

        self.generated_metadata = self.generated_metadata + (("authorization", "Bearer " + self.auth_token),)

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(
            self.refresh_token, "interval", seconds=60
        )
        self.scheduler.start()

    def refresh_token(self):
        try:
            if time.time() + 300 > self.token_expiry_time:
                headers = {
                    "anduril-sandbox-authorization" : f"Bearer {self.sandboxes_token}",
                    "Content-Type": "application/x-www-form-urlencoded"
                    }
                data = {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
                response = requests.post(
                    url=f"https://{self.lattice_endpoint}/api/v1/oauth/token", 
                    headers=headers,
                    data=data
                )
                
                if (response.status_code == 200):
                    self.auth_token = response.json()["access_token"]
                    self.token_expiry_time = time.time() + response.json()["expires_in"]
                    return 
                else: 
                    raise Exception(f"Failed to get auth token: {response.json()}")
        except Exception as err:
            self.logger.error("Failed to refresh token: %s", err)
            return



    async def get_entity(self, entity_id) -> Optional[GetEntityResponse]:
        """
        Asynchronously retrieves an entity from the Lattice API using the provided entity ID.
        Usable wrapper around the get_entity API.
        For more information about this API, please refer to https://docs.anduril.com/sdks/python

        Args:
            entity_id (str): The ID of the entity to retrieve.

        Returns:
            Optional[GetEntityResponse]: The retrieved encased, or None if an error occurred.

        Raises:
            None

        """
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.aio.secure_channel(f"{self.lattice_endpoint}:{self.port}", credentials)
        entity_manager_stub = EntityManagerAPIStub(channel)

        try:
            response = await entity_manager_stub.GetEntity(
                GetEntityRequest(entity_id=entity_id), metadata=self.generated_metadata
            )
            channel.close()
            return response
        except Exception as error:
            self.logger.error(f"lattice api get entity error {error}")
            channel.close()
            return None

    async def publish_entity(self, entity) -> Optional[PublishEntityResponse]:
        """
        Asynchronously publishes an entity to the Lattice API.
        Usable wrapper around the publish_entity API.
        For more information about this API, please refer to https://docs.anduril.com/entity/publishing-your-first-entity

        Args:
            entity (Entity): The entity to be published.

        Returns:
            Optional[PublishEntityResponse]: The response from the Lattice API, or None if an error occurred.

        Raises:
            None
        """
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.aio.secure_channel(f"{self.lattice_endpoint}:{self.port}", credentials)
        entity_manager_stub = EntityManagerAPIStub(channel)

        try:
            response = await entity_manager_stub.PublishEntity(
                PublishEntityRequest(entity=entity),
                metadata=self.generated_metadata
            )
            await channel.close()
            return response
        except Exception as error:
            self.logger.error(f"lattice api publish entity error {error}")
            channel.close()
            return None

    @staticmethod
    def generate_new_entity(vessel_data: VesselData) -> Entity:
        """
        Generates a new entity using the provided VesselData.

        For more information about these data fields, please refer to
        https://docs.anduril.com/reference/models/entitymanager/v1/entity

        Args:
            vessel_data (VesselData): The data containing relevant information about the vessel.

        Returns:
            Entity: The generated entity with the basic attributes filled out:
                - entity_id: The vessel's MMSI.
                - description: A description of the vessel.
                - is_live: A boolean indicating whether the entity is live.
                - created_time: The time the entity was created.
                - expiry_time: The time the entity expires.
                - aliases: The aliases for the entity, including the vessel's MMSI.
                - mil_view: View of the entity.
                - location: The location of the entity, including latitude, longitude, and altitude.
                - ontology: The ontology for the entity, including the template, platform type, and specific type.
                - provenance: The provenance for the entity, including the data type, integration name, and source update time.
                - data_classification: The data classification for the entity, including the level of classification.
        """

        return Entity(
            entity_id=f"{vessel_data.MMSI}",  # Leaving this blank will generate an entityId
            description="Generated by AIS Vessel Traffic Dataset",
            is_live=True,
            created_time=datetime.now(timezone.utc),
            expiry_time=datetime.now(timezone.utc) + timedelta(seconds=EXPIRY_OFFSET_SECONDS),
            aliases=Aliases(
                name=vessel_data.VesselName,
                alternate_ids=[
                    AlternateId(
                        id=str(vessel_data.MMSI),
                        type=AltIdType.ALT_ID_TYPE_MMSI_ID,
                    )
                ],
            ),
            mil_view=MilView(
                disposition=Disposition.DISPOSITION_NEUTRAL,
                environment=Environment.ENVIRONMENT_SURFACE,
            ),
            location=Location(
                position=Position(
                    latitude_degrees=vessel_data.LAT,
                    longitude_degrees=vessel_data.LON,
                ),
            ),
            ontology=Ontology(
                template=Template.TEMPLATE_TRACK,
                # For more information, please refer to https://docs.anduril.com/entity/publishing-your-first-entity#define-ontology
                platform_type="Surface_Vessel",
            ),
            provenance=Provenance(
                data_type="vessel-data",
                integration_name="ais-sample-integration",
                source_update_time=datetime.now(timezone.utc),
            ),
            data_classification=Classification(
                default=ClassificationInformation(
                    level=ClassificationLevels.CLASSIFICATION_LEVELS_UNCLASSIFIED,
                )
            ),
        )
