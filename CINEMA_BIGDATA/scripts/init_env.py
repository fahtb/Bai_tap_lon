"""Create local secrets on a new installation; never overwrite existing .env."""
import base64
import secrets
from pathlib import Path

def main():
    root = Path(__file__).resolve().parents[1]
    target = root / '.env'
    if target.exists():
        print('.env already exists; unchanged')
        return
    values = {'POSTGRES_PASSWORD': secrets.token_hex(24),
              'AIRFLOW_ADMIN_PASSWORD': secrets.token_hex(24),
              'AIRFLOW_SECRET_KEY': secrets.token_hex(32),
              'AIRFLOW_FERNET_KEY': base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()}
    lines = []
    for line in (root / '.env.example').read_text().splitlines():
        key = line.split('=', 1)[0]
        lines.append(key + '=' + values[key] if key in values else line)
    with target.open('x', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print('Created local .env; read AIRFLOW_ADMIN_USER/PASSWORD locally to log in.')

if __name__ == '__main__':
    main()
