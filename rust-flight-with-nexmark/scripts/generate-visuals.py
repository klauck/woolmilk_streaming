import json
import os
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns

def bytes_to_mb(bytes_val):
    """Convert bytes to megabytes (MB = bytes / 1,000,000)"""
    return bytes_val / 1_000_000

def load_json_data(file_path):
    """Load JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from: {file_path}")
        return None

def create_output_directories():
    """Create output directory structure"""
    base_dir = Path("visuals")
    pre_gen_dir = base_dir / "pre-gen"
    real_time_dir = base_dir / "real-time"
    
    pre_gen_dir.mkdir(parents=True, exist_ok=True)
    real_time_dir.mkdir(parents=True, exist_ok=True)
    
    return pre_gen_dir, real_time_dir

def generate_entry_graph(data, output_path, title_suffix=""):
    """Generate comprehensive entry visualization in a single graph"""
    
    # Set up the figure with high DPI for better readability
    fig, ax1 = plt.subplots(figsize=(16, 10), dpi=300)
    
    # Extract data from JSON structure
    batch_ids = [batch['id'] for batch in data['batches']]
    batch_bytes = [batch['bytes'] for batch in data['batches']]
    batch_times = [batch['time'] for batch in data['batches']]
    
    # Convert bytes to MB (using 1,000,000 not 1,048,576)
    batch_mb = [bytes_to_mb(b) for b in batch_bytes]
    
    # Calculate throughput (MB/s) for each batch
    throughput_mb_s = [bytes_to_mb(batch_bytes[i]) / batch_times[i] for i in range(len(batch_bytes))]
    
    # Create bar chart for data sent per batch
    bars = ax1.bar(batch_ids, batch_mb, alpha=0.7, color='steelblue', width=0.6, label='Data Sent (MB)')
    ax1.set_xlabel('Batch ID', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Data Sent (MB)', fontsize=14, fontweight='bold', color='steelblue')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars, batch_mb):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(batch_mb)*0.01,
                f'{value:.1f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Create second y-axis for throughput line chart (hidden axis)
    ax2 = ax1.twinx()
    line1 = ax2.plot(batch_ids, throughput_mb_s, color='red', marker='o', 
                     linewidth=3, markersize=8, markerfacecolor='darkred', 
                     markeredgecolor='red', label='Throughput (MB/s)')
    ax2.set_ylabel('')  # Remove y-axis label
    ax2.tick_params(axis='y', left=False, right=False, labelleft=False, labelright=False)  # Hide ticks
    
    # Add value labels on throughput line points
    for i, (x, y) in enumerate(zip(batch_ids, throughput_mb_s)):
        ax2.annotate(f'{y:.1f}', (x, y), textcoords="offset points", 
                    xytext=(0,10), ha='center', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="red", alpha=0.7, edgecolor='none'))
    
    # Create third y-axis for batch processing time (hidden axis)
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('outward', 60))
    line2 = ax3.plot(batch_ids, batch_times, color='purple', marker='s', 
                     linewidth=2, markersize=6, markerfacecolor='darkviolet', 
                     markeredgecolor='purple', label='Processing Time (s)')
    ax3.set_ylabel('')  # Remove y-axis label
    ax3.tick_params(axis='y', left=False, right=False, labelleft=False, labelright=False)  # Hide ticks
    ax3.spines['right'].set_visible(False)  # Hide the spine
    
    # Add value labels on processing time line points
    for i, (x, y) in enumerate(zip(batch_ids, batch_times)):
        ax3.annotate(f'{y:.3f}s', (x, y), textcoords="offset points", 
                    xytext=(0,-15), ha='center', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="purple", alpha=0.7, edgecolor='none'))
    
    # Calculate summary statistics
    total_data_mb = bytes_to_mb(data['total_data_sent'])
    total_completion_time = data['total_completion_time']
    overall_speed = total_data_mb / total_completion_time
    avg_throughput = np.mean(throughput_mb_s)
    max_throughput = max(throughput_mb_s)
    min_throughput = min(throughput_mb_s)
    
    # Add summary text box
    summary_text = f"""SUMMARY STATISTICS
Total Data Sent: {total_data_mb:.1f} MB
Total Time: {total_completion_time:.2f} seconds
Overall Speed: {overall_speed:.1f} MB/s
Total Batches: {len(data['batches'])}
Avg Throughput: {avg_throughput:.1f} MB/s
Peak Throughput: {max_throughput:.1f} MB/s
Mode: {data['mode']}"""
    
    # Position summary box in upper right
    ax1.text(0.98, 0.98, summary_text, transform=ax1.transAxes,
             fontsize=11, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle="round,pad=0.8", facecolor="lightblue", alpha=0.9),
             fontweight='bold')
    
    # Create combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines3, labels3 = ax3.get_legend_handles_labels()
    ax1.legend(lines1 + lines2 + lines3, labels1 + labels2 + labels3, 
               loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
    
    # Set main title
    plt.title(f'Entry Data Analysis{title_suffix}', fontsize=18, fontweight='bold', pad=20)
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✓ Entry graph saved to: {output_path}")

def generate_entry():
    """Generate entry visualizations for both pre-gen and real-time data"""
    
    # Create output directories
    pre_gen_dir, real_time_dir = create_output_directories()
    
    # Load data from parent directory
    stats_dir = Path("./pi-stats")
    
    # Load pre-gen entry data
    pre_gen_data = load_json_data(stats_dir / "ENTRY-PRE-GEN.json")
    if pre_gen_data:
        generate_entry_graph(pre_gen_data, pre_gen_dir / "entry.png", " - Pre-Generated")
        print(f"✓ Pre-generated entry data processed: {len(pre_gen_data['batches'])} batches")
    else:
        print("✗ Failed to load pre-generated entry data")
    
    # Load real-time entry data
    real_time_data = load_json_data(stats_dir / "ENTRY-REAL-TIME.json")
    if real_time_data:
        generate_entry_graph(real_time_data, real_time_dir / "entry.png", " - Real-Time")
        print(f"✓ Real-time entry data processed: {len(real_time_data['batches'])} batches")
    else:
        print("✗ Failed to load real-time entry data")

def generate_processor_graph(data, output_path, title_suffix=""):
    """Generate processor visualization with timing and throughput in 2x2 grid"""
    
    # Set up the figure with 2x2 subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16), dpi=300)
    
    # Extract and organize all data by batch ID
    batch_data = {}
    
    # Process batches (processing times) - main data available
    for i, batch in enumerate(data['batches']):
        batch_id = i + 1  # Use index-based ID since no 'id' field in new schema
        batch_data[batch_id] = {
            'process_time': batch['time'],
            'send_time': 0,  # Will be filled from send data if available
            'send_bytes': 0
        }
    
    # Process query_processing data if available
    if 'query_processing' in data:
        for item in data['query_processing']:
            if 'id' in item:
                batch_id = item['id']
                if batch_id in batch_data:
                    # Add query processing details if needed
                    if 'time' in item:
                        batch_data[batch_id]['query_time'] = item['time']
    
    # Process send data if available
    if 'send' in data:
        for item in data['send']:
            if 'id' in item:
                batch_id = item['id']
                if batch_id in batch_data:
                    batch_data[batch_id]['send_time'] = item.get('time', 0)
                    if 'bytes' in item:
                        batch_data[batch_id]['send_bytes'] = item['bytes']
    
    # Calculate data for visualization
    batch_ids = sorted(batch_data.keys())
    batch_throughputs = []
    
    # Extract individual times in milliseconds
    process_times_ms = [batch_data[bid]['process_time'] * 1000 for bid in batch_ids]
    send_times_ms = [batch_data[bid]['send_time'] * 1000 for bid in batch_ids]
    
    # Calculate throughput for each batch (using total bytes from JSON if available)
    total_bytes_received = data.get('total_bytes_received', 0)
    total_bytes_sent = data.get('total_bytes_sent', 0)
    
    for batch_id in batch_ids:
        data_point = batch_data[batch_id]
        total_time = data_point['process_time'] + data_point['send_time']
        
        # Estimate bytes per batch if not available per batch
        if data_point['send_bytes'] > 0:
            batch_bytes = data_point['send_bytes']
        else:
            # Estimate based on total data divided by number of batches
            batch_bytes = (total_bytes_received + total_bytes_sent) / len(batch_ids)
        
        if total_time > 0 and batch_bytes > 0:
            throughput = bytes_to_mb(batch_bytes) / total_time
            batch_throughputs.append(throughput)
        else:
            batch_throughputs.append(0)
    
    # Calculate smart labeling intervals based on data size
    num_batches = len(batch_ids)
    if num_batches <= 50:
        label_interval = max(1, num_batches // 10)  # Show ~10 labels
    elif num_batches <= 200:
        label_interval = max(1, num_batches // 8)   # Show ~8 labels
    else:
        label_interval = max(1, num_batches // 6)   # Show ~6 labels for large datasets
    
    # TOP LEFT: Processing Times (main data we have)
    bars1 = ax1.bar(batch_ids, process_times_ms, alpha=0.8, color='steelblue', width=0.8)
    ax1.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Processing Time (ms)', fontsize=12, fontweight='bold')
    ax1.set_title('Processing Times by Batch', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Smart x-axis ticks for large datasets
    if num_batches > 100:
        tick_positions = batch_ids[::max(1, num_batches // 10)]
        ax1.set_xticks(tick_positions)
        ax1.tick_params(axis='x', rotation=45)
    
    # Add strategic value labels
    max_process = max(process_times_ms) if process_times_ms else 1
    for i, (bar, value) in enumerate(zip(bars1, process_times_ms)):
        if i % label_interval == 0 and value > max_process * 0.7:  # Only label high values
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + max_process*0.02,
                    f'{value:.1f}', ha='center', va='bottom', fontsize=8, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.8, edgecolor='none'))
    
    # TOP RIGHT: Send Times (if available)
    bars2 = ax2.bar(batch_ids, send_times_ms, alpha=0.8, color='orange', width=0.8)
    ax2.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Send Time (ms)', fontsize=12, fontweight='bold')
    ax2.set_title('Send Times by Batch', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    if num_batches > 100:
        ax2.set_xticks(tick_positions)
        ax2.tick_params(axis='x', rotation=45)
    
    max_send = max(send_times_ms) if send_times_ms and any(t > 0 for t in send_times_ms) else 1
    for i, (bar, value) in enumerate(zip(bars2, send_times_ms)):
        if i % label_interval == 0 and value > max_send * 0.7:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + max_send*0.02,
                    f'{value:.1f}', ha='center', va='bottom', fontsize=8, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.8, edgecolor='none'))
    
    # BOTTOM LEFT: Throughput (Line Chart with reduced density)
    line_width = 2 if num_batches > 500 else 3
    marker_size = 3 if num_batches > 500 else 6
    
    line = ax3.plot(batch_ids, batch_throughputs, color='red', marker='o', 
                   linewidth=line_width, markersize=marker_size, markerfacecolor='darkred', 
                   markeredgecolor='red', label='Batch Throughput (MB/s)', alpha=0.8)
    
    ax3.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Throughput (MB/s)', fontsize=12, fontweight='bold')
    ax3.set_title('Throughput by Batch', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    
    if num_batches > 100:
        ax3.set_xticks(tick_positions)
        ax3.tick_params(axis='x', rotation=45)
    
    # Add overall throughput line
    overall_total_time = sum(batch_data[bid]['process_time'] + batch_data[bid]['send_time'] for bid in batch_ids)
    overall_total_bytes = total_bytes_received + total_bytes_sent
    overall_throughput = bytes_to_mb(overall_total_bytes) / overall_total_time if overall_total_time > 0 else 0
    
    ax3.axhline(y=overall_throughput, color='darkred', linestyle='--', linewidth=2, 
                label=f'Overall Throughput ({overall_throughput:.1f} MB/s)', alpha=0.8)
    
    # Strategic throughput labels (only show outliers and key points)
    if batch_throughputs:
        throughput_mean = np.mean(batch_throughputs)
        throughput_std = np.std(batch_throughputs)
        threshold = throughput_mean + throughput_std  # Only label above average + 1 std dev
        
        for i, (x, y) in enumerate(zip(batch_ids, batch_throughputs)):
            if i % (label_interval * 2) == 0 or y > threshold:  # Show every 2nd interval or outliers
                ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", 
                            xytext=(0,8), ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.2", facecolor="red", alpha=0.7, edgecolor='none'))
    
    ax3.legend(fontsize=10)
    
    # BOTTOM RIGHT: Summary
    ax4.axis('off')  # Hide the axis for the summary
    
    # Calculate summary statistics
    total_bytes_received_mb = bytes_to_mb(total_bytes_received)
    total_bytes_sent_mb = bytes_to_mb(total_bytes_sent)
    total_processing_time = sum(batch_data[bid]['process_time'] for bid in batch_ids)
    total_send_time = sum(batch_data[bid]['send_time'] for bid in batch_ids)
    total_time = data.get('total_time', 0)
    
    avg_batch_throughput = np.mean(batch_throughputs) if batch_throughputs else 0
    avg_process_time = np.mean(process_times_ms) if process_times_ms else 0
    avg_send_time = np.mean(send_times_ms) if send_times_ms else 0
    
    # Add summary text box
    summary_text = f"""PROCESSOR SUMMARY
{'='*25}
Total Bytes Received: {total_bytes_received_mb:.1f} MB
Total Bytes Sent: {total_bytes_sent_mb:.1f} MB
Overall Throughput: {overall_throughput:.1f} MB/s

Average Times:
Processing: {avg_process_time:.1f} ms
Send: {avg_send_time:.1f} ms

Total Batches: {len(batch_ids)}
Mode: {data['mode']}
"""
    
    # Position summary box in the center of the bottom-right subplot
    ax4.text(0.5, 0.5, summary_text, transform=ax4.transAxes,
             fontsize=10, verticalalignment='center', horizontalalignment='center',
             bbox=dict(boxstyle="round,pad=0.8", facecolor="lightblue", alpha=0.9),
             fontweight='bold')
    
    # Set main title
    fig.suptitle(f'Processor Performance Analysis{title_suffix}', fontsize=20, fontweight='bold')
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✓ Processor graph saved to: {output_path}")

def generate_processor():
    """Generate processor visualizations for both pre-gen and real-time data"""
    
    # Create output directories
    pre_gen_dir, real_time_dir = create_output_directories()
    
    # Load data from parent directory
    stats_dir = Path("./pi-stats")
    
    # Load pre-gen processor data
    pre_gen_data = load_json_data(stats_dir / "PROCESSOR-PRE-GEN.json")
    if pre_gen_data:
        generate_processor_graph(pre_gen_data, pre_gen_dir / "processor.png", " - Pre-Generated")
        print(f"✓ Pre-generated processor data processed: {len(pre_gen_data['batches'])} batches")
    else:
        print("✗ Failed to load pre-generated processor data")
    
    # Load real-time processor data
    real_time_data = load_json_data(stats_dir / "PROCESSOR-REAL-TIME.json")
    if real_time_data:
        generate_processor_graph(real_time_data, real_time_dir / "processor.png", " - Real-Time")
        print(f"✓ Real-time processor data processed: {len(real_time_data['batches'])} batches")
    else:
        print("✗ Failed to load real-time processor data")

def generate_exit_graph(data, output_path, title_suffix=""):
    """Generate exit visualization with 2x2 layout and summary in bottom-right"""
    
    # Set up the figure with 2x2 subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16), dpi=300)
    
    # Extract data from JSON structure
    batch_ids = [batch['id'] for batch in data['batches']]
    batch_bytes = [batch['bytes'] for batch in data['batches']]
    batch_times = [batch['time'] for batch in data['batches']]
    
    # Convert bytes to MB (using 1,000,000 not 1,048,576)
    batch_mb = [bytes_to_mb(b) for b in batch_bytes]
    
    # Calculate throughput (MB/s) for each batch
    throughput_mb_s = [bytes_to_mb(batch_bytes[i]) / batch_times[i] for i in range(len(batch_bytes))]
    
    # Calculate smart labeling intervals based on data size
    num_batches = len(batch_ids)
    if num_batches <= 50:
        label_interval = max(1, num_batches // 10)
    elif num_batches <= 200:
        label_interval = max(1, num_batches // 8)
    else:
        label_interval = max(1, num_batches // 6)
    
    # TOP LEFT: Data Received (MB)
    bars1 = ax1.bar(batch_ids, batch_mb, alpha=0.8, color='darkorange', width=0.8)
    ax1.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Data Received (MB)', fontsize=12, fontweight='bold')
    ax1.set_title('Data Received by Batch', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Smart x-axis management for large datasets
    if num_batches > 100:
        tick_positions = batch_ids[::max(1, num_batches // 10)]
        ax1.set_xticks(tick_positions)
        ax1.tick_params(axis='x', rotation=45)
    
    # Strategic labeling for data received
    max_mb = max(batch_mb) if batch_mb else 1
    for i, (bar, value) in enumerate(zip(bars1, batch_mb)):
        if i % label_interval == 0 and value > max_mb * 0.7:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + max_mb*0.02,
                    f'{value:.1f}', ha='center', va='bottom', fontsize=8, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.8, edgecolor='none'))
    
    # TOP RIGHT: Throughput (MB/s)
    line_width = 2 if num_batches > 500 else 3
    marker_size = 3 if num_batches > 500 else 6
    
    line1 = ax2.plot(batch_ids, throughput_mb_s, color='red', marker='o', 
                    linewidth=line_width, markersize=marker_size, markerfacecolor='darkred', 
                    markeredgecolor='red', label='Throughput (MB/s)', alpha=0.8)
    
    ax2.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Throughput (MB/s)', fontsize=12, fontweight='bold')
    ax2.set_title('Throughput by Batch', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    if num_batches > 100:
        ax2.set_xticks(tick_positions)
        ax2.tick_params(axis='x', rotation=45)
    
    # Strategic throughput labeling
    if throughput_mb_s:
        throughput_mean = np.mean(throughput_mb_s)
        throughput_std = np.std(throughput_mb_s)
        threshold = throughput_mean + throughput_std
        
        for i, (x, y) in enumerate(zip(batch_ids, throughput_mb_s)):
            if i % (label_interval * 2) == 0 or y > threshold:
                ax2.annotate(f'{y:.1f}', (x, y), textcoords="offset points", 
                            xytext=(0,8), ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.2", facecolor="red", alpha=0.7, edgecolor='none'))
    
    # BOTTOM LEFT: Processing Time (seconds)
    bars3 = ax3.bar(batch_ids, batch_times, alpha=0.8, color='purple', width=0.8)
    ax3.set_xlabel('Batch ID', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Processing Time (s)', fontsize=12, fontweight='bold')
    ax3.set_title('Processing Time by Batch', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    
    if num_batches > 100:
        ax3.set_xticks(tick_positions)
        ax3.tick_params(axis='x', rotation=45)
    
    max_time = max(batch_times) if batch_times else 1
    for i, (bar, value) in enumerate(zip(bars3, batch_times)):
        if i % label_interval == 0 and value > max_time * 0.7:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + max_time*0.02,
                    f'{value:.3f}s', ha='center', va='bottom', fontsize=8, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.8, edgecolor='none'))
    
    # BOTTOM RIGHT: Summary (replace with text box)
    ax4.axis('off')  # Hide the axis for the summary
    
    # Calculate summary statistics
    total_data_mb = bytes_to_mb(data['total_data_received'])
    total_completion_time = data['total_completion_time']
    overall_speed = total_data_mb / total_completion_time
    avg_throughput = np.mean(throughput_mb_s)
    max_throughput = max(throughput_mb_s)
    avg_processing_time = np.mean(batch_times)
    
    # Add summary text in the bottom-right subplot
    summary_text = f"""EXIT SUMMARY
{'='*25}

Total Data Received: {total_data_mb:.1f} MB
Total Time: {total_completion_time:.2f} seconds
Overall Speed: {overall_speed:.1f} MB/s

Avg Throughput: {avg_throughput:.1f} MB/s
Peak Throughput: {max_throughput:.1f} MB/s
Avg Processing Time: {avg_processing_time:.3f} s

Total Batches: {len(batch_ids)}
Dataset Size: {'Large' if num_batches > 500 else 'Medium' if num_batches > 100 else 'Small'}
Mode: {data['mode']}"""
    
    # Position summary text in the center of the bottom-right subplot
    ax4.text(0.5, 0.5, summary_text, transform=ax4.transAxes,
             fontsize=12, verticalalignment='center', horizontalalignment='center',
             bbox=dict(boxstyle="round,pad=1.0", facecolor="lightgreen", alpha=0.9),
             fontweight='bold')
    
    # Set main title
    fig.suptitle(f'Exit Data Analysis{title_suffix}', fontsize=20, fontweight='bold')
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✓ Exit graph saved to: {output_path}")

def generate_exit():
    """Generate exit visualizations for both pre-gen and real-time data"""
    
    # Create output directories
    pre_gen_dir, real_time_dir = create_output_directories()
    
    # Load data from parent directory
    stats_dir = Path("./pi-stats")
    
    # Load pre-gen exit data
    pre_gen_data = load_json_data(stats_dir / "EXIT-PRE-GEN.json")
    if pre_gen_data:
        generate_exit_graph(pre_gen_data, pre_gen_dir / "exit.png", " - Pre-Generated")
        print(f"✓ Pre-generated exit data processed: {len(pre_gen_data['batches'])} batches")
    else:
        print("✗ Failed to load pre-generated exit data")
    
    # Load real-time exit data
    real_time_data = load_json_data(stats_dir / "EXIT-REAL-TIME.json")
    if real_time_data:
        generate_exit_graph(real_time_data, real_time_dir / "exit.png", " - Real-Time")
        print(f"✓ Real-time exit data processed: {len(real_time_data['batches'])} batches")
    else:
        print("✗ Failed to load real-time exit data")

def main():
    """Main function to generate all visualizations"""
    print("=" * 60)
    print("🚀 WOOLMILK STREAMING VISUALIZATION GENERATOR")
    print("=" * 60)
    
    # Set matplotlib style for better appearance
    plt.style.use('default')
    plt.rcParams['figure.facecolor'] = 'white'
    plt.rcParams['axes.facecolor'] = 'white'
    
    # Generate entry visualizations
    print("\n📊 Generating Entry Visualizations...")
    generate_entry()
    
    # Generate processor visualizations
    print("\n🔄 Generating Processor Visualizations...")
    generate_processor()
    
    # Generate exit visualizations
    print("\n📤 Generating Exit Visualizations...")
    generate_exit()
    
    print("\n" + "=" * 60)
    print("✅ Visualization generation completed successfully!")
    print("📁 Check the 'visuals' directory for generated graphs")
    print("=" * 60)

if __name__ == "__main__":
    main()