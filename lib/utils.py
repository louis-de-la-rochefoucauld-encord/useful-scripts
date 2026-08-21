"""Shared helpers for the scripts in helpers/."""

import argparse
import os

from encord.user_client import EncordUserClient


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ssh-key-env",
        required=True,
        help="Name of the environment variable holding your Encord SSH private key contents.",
    )
    parser.add_argument(
        "--domain",
        required=True,
        help="Encord API domain, e.g. https://api.encord.com",
    )


def load_client(env_var: str, domain: str) -> EncordUserClient:
    value = os.environ.get(env_var)
    if not value:
        raise SystemExit(
            f'Environment variable {env_var} is not set. Export it with your Encord SSH '
            f'private key contents, e.g. {env_var}="$(cat key.ed25519)".'
        )
    return EncordUserClient.create_with_ssh_private_key(ssh_private_key=value, domain=domain)


def case_id_for_title(data_title: str) -> str:
    """Case ID for a data unit, derived from its title.

    Titles for cloud-registered files are object-store keys, e.g.
    ``DataQC/videos/.../<case_id>_merged.mp4``; the case ID is the file name
    without its extension and any trailing ``_merged`` suffix.
    """
    stem = data_title.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return stem.removesuffix("_merged")
