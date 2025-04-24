import json
import os
import re
import argparse
from pathlib import Path
import time
import random
import requests
import urllib.parse
from collections import Counter


def extract_urls_from_json(file_path, url_file="extracted_urls.txt"):
    """
    Parse a JSON file and extract all 'url_private_download' values from the 'files' property.
    Append these URLs to a text file in the same directory as the input file.

    Args:
        file_path (str or Path): Path to the JSON file to parse

    Returns:
        int: Number of URLs extracted and saved
    """
    # Convert to Path object for easier directory/filename handling
    path = Path(file_path)

    # Create output file path in the same directory
    output_file = path.parent / url_file

    # Read and parse the JSON file
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: {file_path} contains invalid JSON")
        return 0
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return 0
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return 0

    # Extract URLs
    extracted_urls = []

    # Handle both list and dictionary JSON structures
    if isinstance(data, list):
        items = data
    else:
        items = [data]

    # Process each item in the JSON
    for item in items:
        if not isinstance(item, dict):
            continue

        # Check if the item has a 'files' property
        if "files" in item and isinstance(item["files"], list):
            for file_info in item["files"]:
                if isinstance(file_info, dict) and "url_private_download" in file_info:
                    extracted_urls.append(file_info["url_private_download"])

    # Append URLs to output file
    if extracted_urls:
        with open(output_file, "a", encoding="utf-8") as f:
            for url in extracted_urls:
                f.write(f"{url}\n")

    return len(extracted_urls)


def process_url_file(urls_file_path):
    """
    Process a file containing URLs:
    1. Remove duplicate URLs
    2. Identify duplicate filenames
    3. For duplicate filenames, prepend the ID from the URL to make them unique

    Args:
        urls_file_path (str or Path): Path to the file containing URLs

    Returns:
        dict: Dictionary with two keys:
            'unique_urls': List of unique URLs
            'filename_mapping': Dict mapping URLs to their unique filenames
    """
    urls_path = Path(urls_file_path)

    if not urls_path.exists():
        print(f"Error: URL file {urls_path} not found")
        return {"unique_urls": [], "filename_mapping": {}}

    # Read URLs from file
    try:
        with open(urls_path, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading URL file: {str(e)}")
        return {"unique_urls": [], "filename_mapping": {}}

    # Remove duplicates while preserving order
    unique_urls = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    print(f"Found {len(unique_urls)} unique URLs out of {len(urls)} total")

    # Extract filenames and IDs from URLs
    url_info = []
    for url in unique_urls:
        # Parse the URL to extract filename
        parsed_url = urllib.parse.urlparse(url)
        path_parts = parsed_url.path.split("/")

        # Find the index of "download" in the path
        try:
            download_index = path_parts.index("download")
            # Get filename (should be after "download")
            if download_index < len(path_parts) - 1:
                filename = path_parts[download_index + 1]
            else:
                filename = "download.bin"  # Default if no filename after "download"
        except ValueError:
            # If "download" not found in path, use last part of path
            filename = os.path.basename(parsed_url.path)
            if not filename:
                filename = "file.bin"  # Default fallback

        # Extract ID from the path
        # Format: */TAC060NK1-F08FGTZMQ9W/download/*
        id_pattern = r"/([A-Z0-9]+)-([A-Z0-9]+)/download/"
        id_match = re.search(id_pattern, parsed_url.path)

        file_id = ""
        if id_match:
            file_id = f"-{id_match.group(2)}"

        # Handle filenames with no extension
        filename_parts = filename.rsplit(".", 1) if "." in filename else [filename, ""]
        base_name = filename_parts[0]
        extension = (
            f".{filename_parts[1]}"
            if len(filename_parts) > 1 and filename_parts[1]
            else ""
        )

        url_info.append(
            {
                "url": url,
                "original_filename": filename,
                "base_name": base_name,
                "extension": extension,
                "id": file_id,
            }
        )

    # Count occurrences of each filename
    filename_counter = Counter([info["original_filename"] for info in url_info])

    # Create filename mapping for each URL
    filename_mapping = {}
    for info in url_info:
        # If this filename appears more than once, use the ID to make it unique
        if filename_counter[info["original_filename"]] > 1:
            unique_filename = f"{info['base_name']}{info['id']}{info['extension']}"
        else:
            unique_filename = info["original_filename"]

        filename_mapping[info["url"]] = unique_filename

    # Write back deduplicated URLs
    with open(urls_path, "w", encoding="utf-8") as f:
        for url in unique_urls:
            f.write(f"{url}\n")

    # Count how many filenames were modified
    modified_count = sum(
        1
        for orig, new in zip(
            [info["original_filename"] for info in url_info], filename_mapping.values()
        )
        if orig != new
    )

    print(f"Modified {modified_count} filenames to ensure uniqueness")

    return {"unique_urls": unique_urls, "filename_mapping": filename_mapping}


def download_files_from_urls(urls_file_path, download_folder):
    """
    Process a file containing URLs, download the files with random delays,
    and remove successfully downloaded URLs from the file.

    Args:
        urls_file_path (str or Path): Path to the file containing URLs

    Returns:
        tuple: (int, int) - Count of successful downloads and failed downloads
    """
    # Process the URL file to get unique URLs and filename mappings
    url_data = process_url_file(urls_file_path)
    unique_urls = url_data["unique_urls"]
    filename_mapping = url_data["filename_mapping"]

    urls_path = Path(urls_file_path)

    # Create downloads directory in the same folder as the URLs file
    download_dir = urls_path.parent / download_folder
    download_dir.mkdir(exist_ok=True)

    # Track statistics
    success_count = 0
    fail_count = 0
    retry_count = 0
    max_retries = 3
    skipped = 0
    skipped_count = 0

    # Process each URL
    remaining_urls = unique_urls.copy()
    while remaining_urls:
        current_url = remaining_urls[0]

        # Get the unique filename for this URL
        file_name = filename_mapping.get(current_url)
        if not file_name:
            # Fallback if no mapping exists (shouldn't happen)
            parsed_url = urllib.parse.urlparse(current_url)
            file_name = os.path.basename(parsed_url.path)
            if not file_name or file_name == "download":
                file_name = f"download_{hash(current_url) % 10000}.bin"

        # Clean up filename to ensure it's valid
        file_name = "".join(c for c in file_name if c.isalnum() or c in "._- ")
        file_path = Path(download_dir / file_name)

        if file_path.exists():
            #print(f"File {file_name} already exists, skipping download.")
            download_success = True
            remaining_urls.pop(0)
            skipped = skipped + 1
            skipped_count = skipped_count + 1
        else:
            if skipped > 0:
                print(f"Skipped {skipped} files that already exist.")
                skipped = 0
            # Attempt download with retry logic
            retry_count = 0
            print(f"Downloading {file_name} from {current_url[:60]}...")
            download_success = False

        while retry_count <= max_retries and not download_success:
            try:
                # Add a random delay between 1-5 seconds
                delay = random.uniform(1, 5)
                time.sleep(delay)

                # Download the file
                response = requests.get(current_url, stream=True, timeout=30)
                response.raise_for_status()  # Raise an exception for 4xx/5xx status codes

                # Save the file
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                print(f"Successfully downloaded {file_name}")
                download_success = True
                success_count += 1

                # Remove successful URL from the list and file
                remaining_urls.pop(0)

                # Update the URLs file
                with open(urls_path, "w", encoding="utf-8") as f:
                    for url in remaining_urls:
                        f.write(f"{url}\n")

            except Exception as e:
                retry_count += 1

                if retry_count <= max_retries:
                    if retry_count < max_retries:
                        # Wait 30 seconds for first and second failure
                        wait_time = 30
                    else:
                        # Wait 90 seconds for third failure
                        wait_time = 90

                    print(f"Download failed: {str(e)}")
                    print(
                        f"Retry attempt {retry_count}/{max_retries} in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    print(f"Failed to download after {max_retries} attempts: {str(e)}")
                    fail_count += 1
                    remaining_urls.pop(0)

                    # Update the URLs file, marking failed URL with [FAILED] prefix
                    with open(urls_path, "w", encoding="utf-8") as f:
                        f.write(
                            f"[FAILED] {current_url}\n"
                        )  # Keep the failed URL but mark it
                        for url in remaining_urls:
                            f.write(f"{url}\n")

    print(f"\nDownload summary:")
    print(f"  Successful downloads: {success_count}")
    print(f"  Skipped downloads: {skipped_count}")
    print(f"  Failed downloads: {fail_count}")
    print(f"  Total processed: {success_count + fail_count + skipped_count}")

    return success_count, fail_count


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Extract URL_private_download values from JSON files and save them to a text file."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to a JSON file or directory containing JSON files",
    )
    parser.add_argument(
        "-u",
        "--url_file",
        type=str,
        help="Custom URL file name (default: extracted_urls.txt)",
        default="extracted_urls.txt",
    )
    parser.add_argument(
        "--parse", help="Parse JSON files for URLs", default=False, action="store_true"
    )
    parser.add_argument(
        "--download",
        help="Download files from extracted URLs",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--download_folder",
        type=str,
        help="Download subfolder relative to url_file (default: files)",
        default="files",
    )

    # Parse arguments
    args = parser.parse_args()

    # Convert input path to Path object
    input_path = args.path

    total_urls = 0

    if args.parse:
        try:
            if input_path.is_dir():
                # Process all JSON files in the directory (not subdirectories)
                json_files = list(input_path.glob("*.json"))

                if not json_files:
                    print(f"No JSON files found in directory: {input_path}")
                    return

                print(f"Processing {len(json_files)} JSON files in {input_path}")

                for json_file in json_files:
                    count = extract_urls_from_json(json_file)
                    total_urls += count
                    print(f"Extracted {count} URLs from {json_file.name}")

                print(f"Total URLs extracted: {total_urls}")

            elif input_path.is_file():
                # Process single file
                count = extract_urls_from_json(input_path)
                print(f"Extracted {count} URLs to extracted_urls.txt")

            else:
                print(f"Error: '{input_path}' is not a valid file or directory")

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    if args.download:
        if input_path.is_dir():
            # Convert to Path object for easier directory/filename handling
            path = Path(input_path)
        else:
            path = Path(input_path).parent

        # Create output file path in the same directory
        urls_file_path = path / args.url_file
        _, _ = download_files_from_urls(
            urls_file_path=urls_file_path, download_folder=args.download_folder
        )


if __name__ == "__main__":
    main()
