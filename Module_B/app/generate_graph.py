import re
import matplotlib.pyplot as plt
from collections import defaultdict

def create_performance_graph():
    # Dictionary to store execution times for each endpoint
    endpoint_times = defaultdict(list)

    # Regex pattern to extract the API endpoint and the execution time
    log_pattern = re.compile(r"PERFORMANCE: (.*?) - Execution Time: ([\d\.]+) ms")

    # 1. Parse the audit.log file
    try:
        with open('audit.log', 'r') as file:
            for line in file:
                match = log_pattern.search(line)
                if match:
                    endpoint = match.group(1).strip()
                    execution_time = float(match.group(2))
                    endpoint_times[endpoint].append(execution_time)
    except FileNotFoundError:
        print("Error: 'audit.log' not found. Make sure you are in the correct directory.")
        return

    if not endpoint_times:
        print("No performance data found in the log.")
        return

    # 2. Calculate the average time for each endpoint
    endpoints = []
    avg_times = []

    for endpoint, times in endpoint_times.items():
        endpoints.append(endpoint)
        average_time = sum(times) / len(times)
        avg_times.append(average_time)

    # 3. Generate the Graph
    plt.figure(figsize=(10, 6))
    
    # Highlight the slow '/api/bookings' endpoint in red, others in blue
    colors = ['#ff9999' if 'bookings' in e else '#66b3ff' for e in endpoints]
    
    bars = plt.bar(endpoints, avg_times, color=colors, edgecolor='black')

    # Add labels and formatting
    plt.xlabel('API Endpoints', fontweight='bold')
    plt.ylabel('Average Execution Time (ms)', fontweight='bold')
    plt.title('API Endpoint Performance Comparison (Identifying the Bottleneck)', fontweight='bold', fontsize=14)
    plt.xticks(rotation=25, ha='right')
    
    # Add the exact millisecond values on top of each bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 3, f'{yval:.1f} ms', ha='center', va='bottom', fontweight='bold')

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # Save the graph as an image to use in your report
    plt.savefig('bottleneck_graph.png', dpi=300)
    print("Graph successfully generated and saved as 'bottleneck_graph.png'!")
    
    # Display the graph in a window
    plt.show()

if __name__ == '__main__':
    create_performance_graph()