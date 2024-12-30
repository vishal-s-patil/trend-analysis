import matplotlib.pyplot as plt
from io import BytesIO

# Create a simple bar chart using Matplotlib
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


def create_combined_graph(x, y, user_count_map, title, x_axis, y_axis, file_name="combined_graph.png"):
    plt.bar(x, y, label=y_axis, color='skyblue', alpha=0.7)
    
    colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'black', 'yellow', 'cyan', 'magenta', 'r', 'g', 'b', 'c', 'm', 'y', 'k', 'w', '#FF5733', '#33FF57', '#5733FF', 'aqua', 'azure', 'beige', 'chocolate', 'coral', 'gold', 'indigo', 'khaki', 'lavender', 'navy', 'olive', 'teal']
    idx = 0
    if len(user_count_map) != 0: 
        for user, lst in user_count_map.items():
            plt.plot(x, lst, label=user, color=colors[idx], marker='o')
            idx += 1

    step = max(len(x) // 5, 1)
    plt.xticks(ticks=range(0, len(x), step), labels=[x[i] for i in range(0, len(x), step)])
    
    # plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)

    plt.legend()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)  # Reset the buffer position to the beginning
    
    plt.clf()
    
    return img_buffer.getvalue()

# if __name__ == '__main__':
#     title = "All selects day wise trend for 4 weeks"
#     x_axis = "day"
#     y_axis = "total_selects"

#     x = [1, 2, 3, 4, 5]
#     y = [10, 20, 30, 20, 5]
#     z = [3, 7, 4, 8, 3]

#     create_combined_graph(x, y, z, title, x_axis, y_axis)