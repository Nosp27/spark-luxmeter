from datetime import datetime


def format_app(app_data):
    return f"'{app_data['Name']}' since {datetime.fromtimestamp(int(app_data['StartTime'])/1000)} by {app_data['User']}"
