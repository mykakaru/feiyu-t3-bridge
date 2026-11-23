"""SDK/CLI identity namespaces are separate; never infer aliases from names."""
import json
from pathlib import Path

CLI_OWNER = "wokSFfCgAA8AATJbuftE--hahlUVVs0Q"

def resolve(root, sender):
    if sender == CLI_OWNER:
        return {"is_owner": True, "state": "verified_cli_identity", "evidence": "official_identity_whoami"}
    try:
        mapped = json.loads((Path(root)/"identity_mapping.json").read_text(encoding="utf-8"))
        if (sender in mapped.get("owner_ws_userids", []) and mapped.get("verified_at")
                and mapped.get("correlated_msgid") and mapped.get("owner_confirmation")
                and mapped.get("verification") == "authenticated ChatGPT owner confirmed this exact private message"
                and not mapped.get("revoked")):
            return {"is_owner": True, "state": "verified_channel_binding",
                    "evidence": mapped.get("source_ref") or "owner_confirmation:" + mapped["correlated_msgid"]}
    except (OSError, ValueError, TypeError):
        pass
    try:
        data = json.loads((Path(root)/"identity_bindings.json").read_text(encoding="utf-8"))
        for binding in data.get("bindings", []):
            if (binding.get("sdk_userid") == sender and binding.get("cli_owner") == CLI_OWNER
                    and binding.get("verified_by_root") is True
                    and binding.get("source_ref") and not binding.get("revoked")):
                return {"is_owner": True, "state": "verified_channel_binding", "evidence": binding["source_ref"]}
    except (OSError, ValueError, TypeError):
        pass
    return {"is_owner": False, "state": "unbound_sender", "evidence": None}
