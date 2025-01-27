import matplotlib.pyplot as plt
from io import BytesIO

colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'black', 'yellow', 'cyan', 'magenta', 'r', 'g', 'b', 'c', 'm', 'y', 'k', 'w', '#FF5733', '#33FF57', '#5733FF', 'aqua', 'azure', 'beige', 'chocolate', 'coral', 'gold', 'indigo', 'khaki', 'lavender', 'navy', 'olive', 'teal']


def generate_image(x, y):
    y1, y2 = y
    fig, ax = plt.subplots()
    ax.bar(x, y1, label='Category A')
    ax.bar(x, y2, bottom=y1, label='Category B')
    ax.set_title("Monthly Data Comparison")
    ax.set_xlabel("Month")
    ax.set_ylabel("Values")
    ax.legend()

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight')
    img_buffer.seek(0)  # Reset buffer position

    image_binary = img_buffer.getvalue()
    
    # Print the binary content (for verification, print a portion)
    # print(image_binary[:100])  # Print first 100 bytes for verification
    plt.close(fig)
    return image_binary


def create_line_graph(x, y, user_count_map, title, x_axis, y_axis):
    plt.bar(x, y, label=y_axis, color='skyblue', alpha=0.7)
    # plt.plot(x, y, label=y_axis, color='skyblue', marker='o')
    idx = 0
    if len(user_count_map) != 0:
        for user, lst in user_count_map.items():
            plt.plot(x, lst, label=user, color=colors[idx], marker='o')
            idx += 1

    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)

    step = max(len(x) // 5, 1)
    plt.xticks(ticks=[x[i] for i in range(0, len(x), step)], labels=[x[i] for i in range(0, len(x), step)]) # range(0, len(x), step)

    plt.legend()

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='jpeg')
    img_buffer.seek(0)  # Reset the buffer position to the beginning

    plt.clf()

    return img_buffer.getvalue()


def create_combined_graph(x, y, user_count_map, title, x_axis, y_axis, file_name="combined_graph.png"):
    plt.bar(x, y, label=y_axis, color='skyblue', alpha=0.7)

    idx = 0
    if len(user_count_map) != 0: 
        for user, lst in user_count_map.items():
            plt.plot(x, lst, label=user, color=colors[idx], marker='o')
            idx += 1

    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)

    step = max(len(x) // 5, 1)
    plt.xticks(ticks=[x[i] for i in range(0, len(x), step)], labels=[x[i] for i in range(0, len(x), step)])

    plt.legend()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)  # Reset the buffer position to the beginning
    
    plt.clf()
    
    return img_buffer.getvalue()
