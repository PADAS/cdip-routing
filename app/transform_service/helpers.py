from gundi_core import schemas

URN_GUNDI_PREFIX = "urn:gundi:"
URN_GUNDI_FORMAT = "intsrc"


def build_movebank_gundi_urn(gundi_version: str, position: schemas.Position):
    return '.'.join(
        [
            URN_GUNDI_PREFIX + gundi_version,
            URN_GUNDI_FORMAT,
            str(position.integration_id),
            position.device_id
        ]
    )
