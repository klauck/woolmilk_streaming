import os
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from pathlib import Path

def combine_images_side_by_side(old_path, new_path, output_path, title=""):
    """Combine two images side by side"""
    try:
        # Load images
        old_img = Image.open(old_path)
        new_img = Image.open(new_path)
        
        # Get dimensions
        old_width, old_height = old_img.size
        new_width, new_height = new_img.size
        
        # Calculate combined dimensions
        max_height = max(old_height, new_height)
        total_width = old_width + new_width
        
        # Resize images to same height if needed
        if old_height != max_height:
            old_img = old_img.resize((int(old_width * max_height / old_height), max_height))
        if new_height != max_height:
            new_img = new_img.resize((int(new_width * max_height / new_height), max_height))
        
        # Create combined image
        combined = Image.new('RGB', (old_img.width + new_img.width, max_height), 'white')
        combined.paste(old_img, (0, 0))
        combined.paste(new_img, (old_img.width, 0))
        
        # Save combined image
        combined.save(output_path, quality=95, dpi=(300, 300))
        print(f"✓ Combined: {output_path}")
        
    except Exception as e:
        print(f"✗ Error combining {old_path} and {new_path}: {e}")

def create_comparison_images():
    """Create side-by-side comparison images"""
    
    visuals_dir = Path("visuals")
    comparison_dir = Path("comparisons")
    comparison_dir.mkdir(exist_ok=True)
    
    # Define the comparisons to make
    comparisons = [
        ("entry", "Entry Graphs"),
        ("processor", "Processor Graphs"), 
        ("exit", "Exit Graphs")
    ]
    
    modes = ["pre-gen", "real-time"]
    
    for graph_type, title in comparisons:
        for mode in modes:
            old_path = visuals_dir / "old" / mode / f"{graph_type}.png"
            new_path = visuals_dir / "new" / mode / f"{graph_type}.png"
            
            if old_path.exists() and new_path.exists():
                output_path = comparison_dir / f"{graph_type}_{mode}_comparison.png"
                combine_images_side_by_side(old_path, new_path, output_path, f"{title} - {mode.title()}")

def create_markdown_report():
    """Create markdown report with all comparisons"""
    
    comparison_dir = Path("comparisons")
    
    # Check which comparison files exist
    comparisons = []
    
    graph_types = ["entry", "processor", "exit"]
    modes = ["pre-gen", "real-time"]
    
    for graph_type in graph_types:
        for mode in modes:
            comparison_file = comparison_dir / f"{graph_type}_{mode}_comparison.png"
            if comparison_file.exists():
                comparisons.append((graph_type, mode, comparison_file))
    
    # Generate markdown content
    md_content = "# Performance Comparison: Old vs New\n\n"
    
    for graph_type in graph_types:
        md_content += f"## {graph_type.title()} Graphs\n\n"
        
        for mode in modes:
            comparison_file = comparison_dir / f"{graph_type}_{mode}_comparison.png"
            if comparison_file.exists():
                md_content += f"### {mode.title().replace('-', ' ')} Mode\n\n"
                md_content += f"![{graph_type} {mode} comparison](comparisons/{comparison_file.name})\n\n"
                md_content += "---\n\n"
    
    # Write markdown file
    with open("performance_comparison.md", "w") as f:
        f.write(md_content)
    
    print(f"✓ Markdown report created: performance_comparison.md")

def main():
    """Main function"""
    print("=" * 60)
    print("🔄 CREATING PERFORMANCE COMPARISONS")
    print("=" * 60)
    
    print("\n📊 Creating side-by-side comparison images...")
    create_comparison_images()
    
    print("\n📝 Creating markdown comparison report...")
    create_markdown_report()
    
    print("\n" + "=" * 60)
    print("✅ Comparison generation completed!")
    print("📁 Combined images: 'comparisons/' directory")
    print("📄 Markdown report: 'performance_comparison.md'")
    print("=" * 60)

if __name__ == "__main__":
    main()