"""Generate bcrypt password hashes for the auth gate.

Run interactively after `pip install -r requirements.txt`:

  python -m app.generate_auth_hash

For each user, enter a plain password. The script prints the bcrypt
hash to paste into Streamlit Cloud's Secrets UI under [users.<name>].

The plain password is never persisted; only the hash is shown.
"""
from __future__ import annotations

import getpass

try:
    from streamlit_authenticator.utilities.hasher import Hasher
except ImportError as e:
    raise SystemExit(
        "streamlit-authenticator non installé. Lance : pip install -r requirements.txt"
    ) from e


def main() -> None:
    print("Génère un hash bcrypt par utilisateur. Ctrl+C pour finir.\n")
    while True:
        try:
            user = input("Username (ex. arnaud, collegue1) : ").strip().lower()
            if not user:
                continue
            pwd = getpass.getpass(f"Mot de passe pour {user} (saisie masquée) : ")
            if not pwd:
                print("  vide, sauté.")
                continue
            confirm = getpass.getpass("Confirme : ")
            if pwd != confirm:
                print("  ✗ ne correspond pas, recommence.\n")
                continue
            hash_ = Hasher.hash(pwd)
            print(f"\n  → Hash pour '{user}' (copier dans secrets.toml) :\n  {hash_}\n")
        except KeyboardInterrupt:
            print("\nFini.")
            return


if __name__ == "__main__":
    main()
