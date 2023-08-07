from uuid import UUID

URN_GUNDI_PREFIX = "urn:gundi:"
URN_GUNDI_INTSRC_FORMAT = "intsrc"

URN_GUNDI_FORMATS = {
    "integration_source": URN_GUNDI_INTSRC_FORMAT
}


def build_gundi_urn(gundi_version: str, integration_id: UUID, device_id: str, urn_format: str = "integration_source"):
    format_id = URN_GUNDI_FORMATS.get(urn_format, URN_GUNDI_INTSRC_FORMAT)
    return f"{URN_GUNDI_PREFIX}{gundi_version}.{format_id}.{str(integration_id)}.{device_id}"
