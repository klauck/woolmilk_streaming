#!/usr/bin/env python3
"""
Performance Analysis Visualization Script for Pi Cluster Streaming
Generates comprehensive performance graphs from collected stats files.
"""

import json
import os
import glob
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import seaborn as sns

# Set style for better-looking graphs
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_stats_files(stats_dir='./pi-stats'):
    """Load and categorize all stats files"""
    files = glob.glob(os.path.join(stats_dir, '*.json'))
    stats_data = {
        'pre_gen': {'entry': [], 'processor': [], 'exit': []},
        'real_time': {'entry': [], 'processor': [], 'exit': []}
    }
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            filename = os.path.basename(file_path)
            
            # Extract component from filename (ENTRY, PROCESSOR, EXIT)
            if 'ENTRY' in filename:
                component = 'entry'
            elif 'PROCESSOR' in filename:
                component = 'processor'
            elif 'EXIT' in filename:
                component = 'exit'
            else:
                continue  # Skip unknown components
            
            # Extract mode and timestamp from filename
            if 'PRE_GEN' in filename:
                category = 'pre_gen'
            elif 'REAL_TIME' in filename:
                category = 'real_time'
            else:
                # Fallback: use timestamp for categorization
                try:
                    timestamp = int(filename.split('-')[-1].split('.')[0])
                    category = 'pre_gen' if timestamp < 1751301300 else 'real_time'
                except (ValueError, IndexError):
                    continue
            
            # Extract timestamp for sorting
            try:
                timestamp = int(filename.split('-')[-1].split('.')[0])
            except (ValueError, IndexError):
                timestamp = 0
            
            stats_data[category][component].append({
                'file': filename,
                'data': data,
                'timestamp': timestamp
            })
                
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    return stats_data

def create_throughput_comparison(ax, data, title):
    """Create throughput comparison chart"""
    components = []
    throughputs = []
    colors = ['#2E86AB', '#A23B72', '#F18F01']
    
    for i, (comp, files) in enumerate(data.items()):
        if files:
            # Take the most recent file for each component
            latest = max(files, key=lambda x: x['timestamp'])
            comp_data = latest['data']
            
            if comp == 'processor':
                # For processor, calculate throughput from raw data (not summary)
                if 'data' in comp_data:
                    proc_data = comp_data['data']
                    
                    # Calculate total times from individual data points
                    total_receive_time = sum([d['time_seconds'] for d in proc_data.get('receive', [])])
                    total_send_time = sum([d['time_seconds'] for d in proc_data.get('send', [])])
                    total_query_time = sum([p['query_execution_time_seconds'] for p in proc_data.get('processing', [])])
                    
                    # Calculate total data volume from receive data
                    total_input_bytes = sum([d['bytes'] for d in proc_data.get('receive', [])])
                    total_output_bytes = sum([d['bytes'] for d in proc_data.get('send', [])])
                    total_processed_mb = (total_input_bytes + total_output_bytes) / 1_000_000.0
                    
                    # Total time = receive + query + send
                    total_time = total_receive_time + total_query_time + total_send_time
                    throughput = total_processed_mb / total_time if total_time > 0 else 0
                else:
                    throughput = 0
            else:
                # For entry and exit, use existing logic
                summary = comp_data.get('summary', {})
                if 'overall_throughput_mbps' in summary:
                    throughput = summary['overall_throughput_mbps']
                else:
                    throughput = 0
            
            components.append(comp.title())
            throughputs.append(throughput)
    
    bars = ax.bar(components, throughputs, color=colors[:len(components)])
    ax.set_ylabel('True Throughput (MB/s)')
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, throughput in zip(bars, throughputs):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{throughput:.1f}', ha='center', va='bottom', fontweight='bold')

def create_processing_time_analysis(ax, data, title):
    """Create processing time analysis chart"""
    if not data.get('processor'):
        ax.text(0.5, 0.5, 'No Processor Data Available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)
        return
    
    latest = max(data['processor'], key=lambda x: x['timestamp'])
    proc_data = latest['data']
    
    if 'data' in proc_data and 'processing' in proc_data['data']:
        processing_stats = proc_data['data']['processing']
        
        chunk_ids = [p['chunk_id'] for p in processing_stats]
        query_times = [p['query_execution_time_seconds'] * 1000 for p in processing_stats]  # Convert to ms
        processing_times = [p['processing_time_seconds'] * 1000 for p in processing_stats]
        
        ax.plot(chunk_ids, query_times, label='Query Execution Time', marker='o', markersize=3)
        ax.plot(chunk_ids, processing_times, label='Total Processing Time', marker='s', markersize=3)
        
        ax.set_xlabel('Chunk ID')
        ax.set_ylabel('Time (ms)')
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, 'No Processing Time Data Available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)

def create_data_volume_chart(ax, data, title):
    """Create data volume processing chart"""
    components = []
    volumes = []
    colors = ['#2E86AB', '#A23B72', '#F18F01']
    
    for comp, files in data.items():
        if files:
            latest = max(files, key=lambda x: x['timestamp'])
            summary = latest['data'].get('summary', {})
            
            if 'total_mb' in summary:
                volume = summary['total_mb']
            elif 'receive' in summary and 'total_mb' in summary['receive']:
                volume = summary['receive']['total_mb']
            else:
                volume = 0
            
            components.append(comp.title())
            volumes.append(volume)
    
    bars = ax.bar(components, volumes, color=colors[:len(components)])
    ax.set_ylabel('Data Volume (MB)')
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, volume in zip(bars, volumes):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{volume:.0f}', ha='center', va='bottom', fontweight='bold')

def create_selectivity_analysis(ax, data, title):
    """Create query selectivity analysis"""
    if not data.get('processor'):
        ax.text(0.5, 0.5, 'No Processor Data Available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)
        return
    
    latest = max(data['processor'], key=lambda x: x['timestamp'])
    proc_data = latest['data']
    
    if 'data' in proc_data and 'processing' in proc_data['data']:
        processing_stats = proc_data['data']['processing']
        
        chunk_ids = [p['chunk_id'] for p in processing_stats]
        selectivity = [p['selectivity_percentage'] for p in processing_stats]
        
        ax.plot(chunk_ids, selectivity, marker='o', markersize=4, linewidth=2)
        ax.set_xlabel('Chunk ID')
        ax.set_ylabel('Selectivity (%)')
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 110)
    else:
        ax.text(0.5, 0.5, 'No Selectivity Data Available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)

def create_latency_distribution(ax, data, title):
    """Create latency distribution analysis"""
    all_times = []
    labels = []
    
    for comp, files in data.items():
        if files:
            latest = max(files, key=lambda x: x['timestamp'])
            comp_data = latest['data']
            
            if 'data' in comp_data:
                if isinstance(comp_data['data'], list):
                    times = [d['time_seconds'] * 1000 for d in comp_data['data']]  # Convert to ms
                elif comp == 'processor' and 'processing' in comp_data['data']:
                    times = [p['query_execution_time_seconds'] * 1000 for p in comp_data['data']['processing']]
                else:
                    continue
                
                all_times.extend(times)
                labels.extend([comp.title()] * len(times))
    
    if all_times:
        df = pd.DataFrame({'Time (ms)': all_times, 'Component': labels})
        
        # Create box plot
        components = df['Component'].unique()
        positions = range(len(components))
        
        box_data = [df[df['Component'] == comp]['Time (ms)'].values for comp in components]
        bp = ax.boxplot(box_data, positions=positions, patch_artist=True)
        
        colors = ['#2E86AB', '#A23B72', '#F18F01']
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax.set_xticklabels(components)
        ax.set_ylabel('Latency (ms)')
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, 'No Latency Data Available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)

def create_performance_summary_table(ax, data, title):
    """Create performance summary table"""
    summary_data = []
    
    for comp, files in data.items():
        if files:
            latest = max(files, key=lambda x: x['timestamp'])
            comp_data = latest['data']
            
            if comp == 'processor':
                # Calculate from raw data (not summary)
                if 'data' in comp_data:
                    proc_data = comp_data['data']
                    
                    # Calculate totals from individual data points
                    total_receive_time = sum([d['time_seconds'] for d in proc_data.get('receive', [])])
                    total_send_time = sum([d['time_seconds'] for d in proc_data.get('send', [])])
                    total_query_time = sum([p['query_execution_time_seconds'] for p in proc_data.get('processing', [])])
                    
                    # Calculate data volumes and metrics
                    total_input_rows = sum([d['rows'] for d in proc_data.get('receive', [])])
                    total_output_rows = sum([p['output_rows'] for p in proc_data.get('processing', [])])
                    total_input_bytes = sum([d['bytes'] for d in proc_data.get('receive', [])])
                    total_output_bytes = sum([d['bytes'] for d in proc_data.get('send', [])])
                    total_processed_mb = (total_input_bytes + total_output_bytes) / 1_000_000.0
                    
                    # Calculate throughput and metrics
                    total_time = total_receive_time + total_query_time + total_send_time
                    true_throughput = total_processed_mb / total_time if total_time > 0 else 0
                    selectivity = (total_output_rows / total_input_rows * 100) if total_input_rows > 0 else 0
                    avg_query_time = (total_query_time / len(proc_data.get('processing', []))) if proc_data.get('processing') else 0
                    
                    row = [
                        comp.title(),
                        f"{total_input_rows:,}",
                        f"{total_output_rows:,}",
                        f"{selectivity:.1f}%",
                        f"{true_throughput:.1f}",
                        f"{avg_query_time*1000:.1f}"
                    ]
                else:
                    row = [comp.title(), "-", "-", "-", "-", "-"]
            else:
                summary = comp_data.get('summary', {})
                row = [
                    comp.title(),
                    f"{summary.get('total_rows', 0):,}",
                    "-",
                    "-",
                    f"{summary.get('overall_throughput_mbps', 0):.1f}",
                    f"{np.mean([d['time_seconds'] for d in comp_data.get('data', [])]) * 1000:.1f}" if 'data' in comp_data else "-"
                ]
            summary_data.append(row)
    
    if summary_data:
        headers = ['Component', 'Input Rows', 'Output Rows', 'Selectivity', 'True Throughput\n(MB/s)', 'Avg Time\n(ms)']
        
        ax.axis('tight')
        ax.axis('off')
        
        table = ax.table(cellText=summary_data, colLabels=headers, 
                        cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.5)
        
        # Style the table
        for i in range(len(headers)):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        for i in range(1, len(summary_data) + 1):
            for j in range(len(headers)):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#f0f0f0')
    
    ax.set_title(title, pad=20)

def generate_performance_graphs():
    """Main function to generate performance visualization"""
    print("Loading statistics files...")
    stats_data = load_stats_files()
    
    # Create two main figures for Pre-Generated and Real-Time modes
    modes = [('pre_gen', 'Pre-Generated Mode'), ('real_time', 'Real-Time Mode')]
    
    for mode_key, mode_title in modes:
        data = stats_data[mode_key]
        
        # Skip if no data available for this mode
        if not any(data.values()):
            print(f"No data available for {mode_title}")
            continue
        
        print(f"Generating graphs for {mode_title}...")
        
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 16))
        fig.suptitle(f'Pi Cluster Streaming Performance Analysis - {mode_title}', 
                    fontsize=20, fontweight='bold', y=0.98)
        
        # Create 3x2 subplot layout
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # 1. Throughput Comparison
        ax1 = fig.add_subplot(gs[0, 0])
        create_throughput_comparison(ax1, data, 'Component Throughput Comparison')
        
        # 2. Data Volume Comparison
        ax2 = fig.add_subplot(gs[0, 1])
        create_data_volume_chart(ax2, data, 'Data Volume Processed')
        
        # 3. Processing Time Analysis
        ax3 = fig.add_subplot(gs[1, 0])
        create_processing_time_analysis(ax3, data, 'Query Processing Time Analysis')
        
        # 4. Query Selectivity Analysis
        ax4 = fig.add_subplot(gs[1, 1])
        create_selectivity_analysis(ax4, data, 'Query Selectivity Over Time')
        
        # 5. Latency Distribution
        ax5 = fig.add_subplot(gs[2, 0])
        create_latency_distribution(ax5, data, 'Latency Distribution by Component')
        
        # 6. Performance Summary Table
        ax6 = fig.add_subplot(gs[2, 1])
        create_performance_summary_table(ax6, data, 'Performance Summary')
        
        # Add timestamp and metadata
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fig.text(0.02, 0.02, f'Generated: {timestamp} | Pi Cluster Performance Analysis', 
                fontsize=10, alpha=0.7)
        
        # Save figure
        filename = f'pi_cluster_performance_{mode_key}.png'
        plt.savefig(filename, dpi=500, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print(f"Saved: {filename}")
        
        plt.close()

def print_summary_stats(stats_data):
    """Print summary statistics to console"""
    print("\n" + "="*60)
    print("PERFORMANCE SUMMARY")
    print("="*60)
    
    for mode_key, mode_title in [('pre_gen', 'Pre-Generated'), ('real_time', 'Real-Time')]:
        data = stats_data[mode_key]
        if not any(data.values()):
            continue
            
        print(f"\n{mode_title} Mode:")
        print("-" * 30)
        
        for comp, files in data.items():
            if files:
                latest = max(files, key=lambda x: x['timestamp'])
                comp_data = latest['data']
                
                print(f"\n{comp.title()}:")
                if comp == 'processor':
                    # Calculate from raw data (not summary)
                    if 'data' in comp_data:
                        proc_data = comp_data['data']
                        
                        # Calculate totals from individual data points
                        total_receive_time = sum([d['time_seconds'] for d in proc_data.get('receive', [])])
                        total_send_time = sum([d['time_seconds'] for d in proc_data.get('send', [])])
                        total_query_time = sum([p['query_execution_time_seconds'] for p in proc_data.get('processing', [])])
                        
                        # Calculate data volumes and metrics
                        total_input_rows = sum([d['rows'] for d in proc_data.get('receive', [])])
                        total_output_rows = sum([p['output_rows'] for p in proc_data.get('processing', [])])
                        total_input_bytes = sum([d['bytes'] for d in proc_data.get('receive', [])])
                        total_output_bytes = sum([d['bytes'] for d in proc_data.get('send', [])])
                        total_processed_mb = (total_input_bytes + total_output_bytes) / 1_000_000.0
                        
                        # Calculate throughput and metrics
                        total_time = total_receive_time + total_query_time + total_send_time
                        true_throughput = total_processed_mb / total_time if total_time > 0 else 0
                        selectivity = (total_output_rows / total_input_rows * 100) if total_input_rows > 0 else 0
                        avg_query_time = (total_query_time / len(proc_data.get('processing', []))) if proc_data.get('processing') else 0
                        receive_only_throughput = (total_input_bytes / 1_000_000.0) / total_receive_time if total_receive_time > 0 else 0
                        
                        print(f"  True Throughput: {true_throughput:.1f} MB/s (receive + processing + send)")
                        print(f"  Input Throughput (receive only): {receive_only_throughput:.1f} MB/s")
                        print(f"  Input Rows: {total_input_rows:,}")
                        print(f"  Output Rows: {total_output_rows:,}")
                        print(f"  Selectivity: {selectivity:.1f}%")
                        print(f"  Avg Query Time: {avg_query_time*1000:.1f} ms")
                        print(f"  Total Processing Time: {total_query_time:.2f}s")
                        print(f"  Total Receive Time: {total_receive_time:.2f}s")
                        print(f"  Total Send Time: {total_send_time:.2f}s")
                        print(f"  Total End-to-End Time: {total_time:.2f}s")
                    else:
                        print("  No detailed data available")
                else:
                    summary = comp_data.get('summary', {})
                    print(f"  Throughput: {summary.get('overall_throughput_mbps', 0):.1f} MB/s")
                    print(f"  Total Rows: {summary.get('total_rows', 0):,}")
                    print(f"  Total Data: {summary.get('total_mb', 0):.1f} MB")

if __name__ == "__main__":
    print("Pi Cluster Performance Analysis")
    print("=" * 40)
    
    # Check if stats directory exists
    if not os.path.exists('./pi-stats'):
        print("Error: ./pi-stats directory not found!")
        print("Please run the data collection script first.")
        exit(1)
    
    # Load data and print summary
    stats_data = load_stats_files()
    print_summary_stats(stats_data)
    
    # Generate graphs
    generate_performance_graphs()
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("Generated files:")
    for filename in ['pi_cluster_performance_pre_gen.png', 'pi_cluster_performance_real_time.png']:
        if os.path.exists(filename):
            print(f"  ✓ {filename}")
        else:
            print(f"  ✗ {filename} (no data)")