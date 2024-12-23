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

def create_combined_graph(x, y, z, title, x_axis, y_axis, file_name="combined_graph.png"):
    plt.bar(x, y, label="Bar Graph (Y)", color='skyblue', alpha=0.7)
    
    plt.plot(x, z, label="Line Graph (Z)", color='red', marker='o')
    
    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    
    plt.legend()
    
    plt.savefig(file_name)
    
    plt.clf()

if __name__ == '__main__':
    title = "All selects day wise trend for 4 weeks"
    x_axis = "day"
    y_axis = "total_selects"
    
    x = [1, 2, 3, 4, 5]
    y = [10, 20, 30, 20, 5]
    z = [3, 7, 4, 8, 3]

    create_combined_graph(x, y, z, title, x_axis, y_axis)