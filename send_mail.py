import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from generate_graph import generate_image

def send_email_with_titles_and_images(title_image_pairs, mail_config, subject):
    msg = MIMEMultipart()
    msg['From'] = mail_config["sender_email"]
    msg['To'] = ", ".join(mail_config["receiver_emails"]) # ", ".join(mail_config["receiver_emails"])
    msg['Subject'] = subject

    body = "<html><body>"

    # Use a table to arrange images in two per row
    body += "<table style='width:100%;'>"
    
    for idx, (title, img) in enumerate(title_image_pairs, start=1):
        if idx % 2 == 1:  # Start a new row for each pair of images
            body += "<tr>"
        
        body += f"<td style='width:50%; padding:10px; text-align:center;'>"
        body += f"<h2>{title}</h2>"
        
        img = MIMEImage(img, _subtype='png')
        img.add_header('Content-ID', f'<image{idx}>')
        msg.attach(img)

        body += f'<img src="cid:image{idx}" alt="Image {idx}" style="width:300px;height:auto;">'
        body += "</td>"

        if idx % 2 == 0:  # End the row after every pair of images
            body += "</tr>"
    
    if len(title_image_pairs) % 2 != 0:  # If there's an odd number of images, close the last row
        body += "</tr>"
    
    body += "</table>"

    body += "</body></html>"

    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP(mail_config["smtp_server"], mail_config["smtp_port"]) as server:
            server.starttls()  # Secure the connection
            server.login(mail_config["sender_email"], mail_config["password"])
            server.sendmail(mail_config["sender_email"], mail_config["receiver_emails"], msg.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    pass
    # x1 = ['Jan', 'Feb', 'Mar']
    # y1 = [[10, 20, 30], [15, 25, 35]]
    # img1 = generate_image(x1, y1)

    # x2 = ['Mar', 'Apr', 'May']
    # y2 = [[15, 25, 35], [10, 20, 30]]
    # img2 = generate_image(x2, y2)

    # title_image_pairs = [
    #     ("image1", img1),
    #     ("image2", img2)
    # ]

    # send_email_with_titles_and_images(title_image_pairs)