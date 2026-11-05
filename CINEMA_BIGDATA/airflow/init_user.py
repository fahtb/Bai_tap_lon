"""Create the configured local demo login once; do not print its password."""
import os
from airflow.www.app import create_app

app = create_app()
with app.app_context():
    sm = app.appbuilder.sm
    username = os.environ['AIRFLOW_ADMIN_USER']
    if not sm.find_user(username=username):
        user = sm.add_user(username, 'Cinema', 'Admin', 'admin@example.invalid',
                           sm.find_role('Admin'), os.environ['AIRFLOW_ADMIN_PASSWORD'])
        if not user:
            raise RuntimeError('Could not create Airflow user')
    print('Airflow login is ready; credentials are in local .env')
