from uuid import UUID

URN_GUNDI_PREFIX = "urn:gundi:"
URN_GUNDI_FORMAT = "intsrc"


def build_gundi_urn(gundi_version: str, integration_id: UUID, device_id: str):
    return f"{URN_GUNDI_PREFIX}{gundi_version}.{URN_GUNDI_FORMAT}.{str(integration_id)}.{device_id}"
