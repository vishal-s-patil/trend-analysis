import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from generate_graph import generate_image

def send_email_with_titles_and_images(lst_title_image_pairs, mail_config, items_per_row, subject):
    msg = MIMEMultipart()
    msg['From'] = mail_config["sender_email"]
    msg['To'] = ", ".join(mail_config["receiver_emails"])
    msg['Subject'] = subject

    body = "<html><body>"
    body += "<table style='width:100%;'>"
    
    for (title, title_image_pairs) in lst_title_image_pairs:
        body += f"<h2>{title}</h2>"
        for idx, (title, img) in enumerate(title_image_pairs, start=1):
            if (idx - 1) % items_per_row == 0:
                body += "<tr>"
            
            body += f"<td style='width:{100 // items_per_row}%; padding:10px; text-align:center;'>"
            
            img = MIMEImage(img, _subtype='png')
            img.add_header('Content-ID', f'<image{idx}>')
            msg.attach(img)

            body += f'<img src="cid:image{idx}" alt="Image {idx}" style="width:300px;height:auto;">'
            body += "</td>"

            if idx % items_per_row == 0:
                body += "</tr>"
        
        if len(title_image_pairs) % items_per_row != 0:
            body += "</tr>"
        
        body += "</table>"
        body += "</body></html>"

    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP(mail_config["smtp_server"], mail_config["smtp_port"]) as server:
            server.starttls()
            server.login(mail_config["sender_email"], mail_config["password"])
            server.sendmail(mail_config["sender_email"], mail_config["receiver_emails"], msg.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    pass