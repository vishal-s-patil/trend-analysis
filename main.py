from dotenv import load_dotenv
import os
from vertica import create_connection, read
#from mail import send_email_with_titles_and_images
load_dotenv()

# mail config 
mail_config = {
    "smtp_server": 'smtp.gmail.com',
    "smtp_port": 587,
    "sender_email": 'vspatil8123@gmail.com',
    "receiver_email": 'vspatil8123@gmail.com',
    "password": os.getenv('GMAIL_APP_PASSWORD')
}

vertica_config = {
    "host": "vertica-cluster-url-02-prod-us.netcorein.com",
    "user": "devops",
    "password": "v?9SaX~cWc8-L~#",
    "database": "smartech",
    "port": 5433,
    "autoCommit": False
}

# -U devops -d smartech --password='v?9\SaX~cWc8-L~#' -h 


if __name__ == "__main__":
    print("Generation graphs")

    vertica_connection = create_connection(vertica_config["host"], vertica_config["user"], vertica_config["password"], vertica_config["database"], vertica_config["port"], vertica_config["autoCommit"])

    query = """select
        date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
        count(1)
        from netstats.trend_analysis 
        where date_trunc_day >= '2024-11-30' 
        group by date_trunc_day 
        order by date_trunc_day;"""

    columns = ["date", "count"]

    read(vertica_connection, query, columns)
    # send_email_with_titles_and_images(title_image_pairs, mail_config)
