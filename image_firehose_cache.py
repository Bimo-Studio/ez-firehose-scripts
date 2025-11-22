#!/usr/bin/env python3
"""
Image Firehose Listener with Cache
Logs verbose CAR parsing to file, important stats to console
MAKE SURE you run the create session script before you try running this
This script finds images in the firehose, copies them down locally, then serves them locally on port 8000
Useful as a starting point for generating your own feed of puppy images, analysis, or whatever
Video and Audio scripts are coming later
"""

import asyncio
import re
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
import requests
import base58
from multiformats import *
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR, Client

# Set up separate loggers for console and file
console_logger = logging.getLogger('console')
file_logger = logging.getLogger('file')

# Console handler (INFO level for important messages)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_formatter)

# File handler (DEBUG level for detailed logs)
file_handler = logging.FileHandler('firehose_debug.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)

# Configure loggers
console_logger.addHandler(console_handler)
console_logger.setLevel(logging.INFO)

file_logger.addHandler(file_handler)
file_logger.setLevel(logging.DEBUG)

# Also configure root logger for third-party libraries
logging.basicConfig(
    level=logging.WARNING,  # Only show warnings and errors from libs
    handlers=[console_handler],
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ImageFirehoseCache:
    def __init__(self, cache_dir: str = "image_cache", max_cache_size: int = 1000):
        self.cache_dir = Path(cache_dir)
        self.max_cache_size = max_cache_size
        self.metadata_file = self.cache_dir / "metadata.json"
        self.images_dir = self.cache_dir / "images"

        # Create directories
        self.cache_dir.mkdir(exist_ok=True)
        self.images_dir.mkdir(exist_ok=True)

        # Session client for downloading
        self.session_client = Client()
        self._setup_session()

        # Cache state
        self.image_metadata = self._load_metadata()
        self.current_size = 0
        self.stats = {
            'posts_processed': 0,
            'images_found': 0,
            'images_downloaded': 0,
            'errors': 0,
            'total_messages': 0,
            'start_time': datetime.now().isoformat()
        }

        console_logger.info(f"Image cache initialized at {self.cache_dir}")

    def _setup_session(self):
        """Setup session from session.txt file with refresh handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open('session.txt', 'r') as f:
                    session_string = f.read().strip()

                if not session_string:
                    raise ValueError("Empty session file")

                self.session_client.login(session_string=session_string)

                # Verify session works by making a simple API call
                self.session_client.app.bsky.actor.get_profile({
                    'actor': self.session_client.me.did
                })

                console_logger.info("✅ Session authenticated and verified")
                return

            except Exception as e:
                console_logger.warning(f"❌ Session setup failed (attempt {attempt + 1}/{max_retries}): {e}")

                if "ExpiredToken" in str(e) or "revoked" in str(e):
                    console_logger.info("🔄 Session expired, please run create_session.py to generate a new one")
                    raise

                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    console_logger.error("💥 Failed to setup session after multiple attempts")
                    raise

    def _load_metadata(self):
        """Load existing metadata from file"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                console_logger.info(f"📂 Loaded {len(data)} existing images from cache")
                return data
            except Exception as e:
                console_logger.error(f"Error loading metadata: {e}")
        return []

    def _save_metadata(self):
        """Save metadata to file"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.image_metadata, f, indent=2)
        except Exception as e:
            console_logger.error(f"Error saving metadata: {e}")

    def _download_image(self, image_ref, author_did, image_id):
        """Simplified image download approach"""
        try:
            console_logger.info(f"🔄 Attempting to download image {image_ref} from {author_did}")

            # Use a direct HTTP approach as fallback
            import requests

            # Try to get the PDS endpoint from the DID
            try:
                # Resolve DID document to find PDS service
                resolve_url = f"https://plc.directory/{author_did}"
                response = requests.get(resolve_url, timeout=10)
                if response.status_code == 200:
                    did_doc = response.json()
                    # Look for PDS service in DID document
                    for service in did_doc.get('service', []):
                        if service.get('type') == 'AtprotoPersonalDataServer':
                            pds_url = service.get('serviceEndpoint')
                            if pds_url:
                                # Try to download from the specific PDS
                                blob_url = f"{pds_url}/xrpc/com.atproto.sync.getBlob?did={author_did}&cid={image_ref}"
                                blob_response = requests.get(blob_url, timeout=30)
                                if blob_response.status_code == 200:
                                    blob_data = blob_response.content
                                    extension = self._guess_extension(blob_data)
                                    filename = f"{image_id}{extension}"
                                    filepath = self.images_dir / filename

                                    with open(filepath, 'wb') as f:
                                        f.write(blob_data)

                                    self.stats['images_downloaded'] += 1
                                    console_logger.info(f"✅ Successfully downloaded image via PDS: {filename}")
                                    return filename
            except Exception as e:
                console_logger.warning(f"Direct PDS download failed: {e}")

            # Fallback to ATProto client
            try:
                blob_data = self.session_client.com.atproto.sync.get_blob({
                    'did': author_did,
                    'cid': image_ref
                })

                if blob_data:
                    extension = self._guess_extension(blob_data)
                    filename = f"{image_id}{extension}"
                    filepath = self.images_dir / filename

                    with open(filepath, 'wb') as f:
                        f.write(blob_data)

                    self.stats['images_downloaded'] += 1
                    console_logger.info(f"✅ Successfully downloaded image via client: {filename}")
                    return filename

            except Exception as e:
                console_logger.error(f"Client download also failed: {e}")

        except Exception as e:
            console_logger.error(f"All download methods failed for {image_ref}: {e}")
            self.stats['errors'] += 1

        return None

    def _get_author_handle(self, author_did):
        """Get author handle from DID with caching"""
        # Simple cache to avoid repeated lookups
        if not hasattr(self, '_handle_cache'):
            self._handle_cache = {}

        if author_did in self._handle_cache:
            return self._handle_cache[author_did]

        try:
            profile = self.session_client.app.bsky.actor.get_profile({'actor': author_did})
            handle = profile.handle
            self._handle_cache[author_did] = handle
            return handle
        except Exception as e:
            console_logger.warning(f"Could not get handle for {author_did}: {e}")
            return author_did  # Fallback to DID

    def _guess_extension(self, blob_data):
        """Guess file extension from blob data"""
        if blob_data.startswith(b'\xff\xd8\xff'):
            return '.jpg'
        elif blob_data.startswith(b'\x89PNG\r\n\x1a\n'):
            return '.png'
        elif blob_data.startswith(b'GIF'):
            return '.gif'
        elif blob_data.startswith(b'RIFF') and blob_data[8:12] == b'WEBP':
            return '.webp'
        else:
            return '.jpg'  # Default


    def add_image(self, image_data, post_text, author_did, author_handle, timestamp, hashtags):
        """Add a new image to the cache - handles both dict and model image data"""
        try:
            image_id = f"img_{int(time.time() * 1000)}_{len(self.image_metadata)}"

            # Extract image reference based on data type
            image_ref = None
            alt_text = ""

            if isinstance(image_data, dict):
                # Handle dict image data
                image_ref = image_data.get('image', {}).get('ref')
                alt_text = image_data.get('alt', '')
            else:
                # Handle model image data (original)
                if hasattr(image_data, 'image') and hasattr(image_data.image, 'ref'):
                    image_ref = image_data.image.ref
                alt_text = getattr(image_data, 'alt', '')

            # Convert image_ref to string if it's bytes
            if isinstance(image_ref, bytes):
                console_logger.info(f"Converting bytes image ref to string: {image_ref[:20]}...")
                try:
                    # The bytes are a full CID v1 - convert the ENTIRE thing
                    # Format: [0x01 (cid version), 0x55 (raw codec), 0x12 (sha2-256), 0x20 (32 bytes), ...hash...]

                    # Use multiformats library to properly decode the CID
                    from multiformats import CID
                    cid = CID.decode(image_ref)
                    cid_string = cid.encode('base32')  # Use base32 for ATProto compatibility
                    console_logger.info(f"Converted CID to base32: {cid_string}")
                    image_ref = cid_string

                except Exception as e:
                    console_logger.error(f"Failed to convert bytes ref with multiformats: {e}")
                    # Fallback: try base58 for the entire CID
                    try:
                        cid_string = base58.b58encode(image_ref).decode('ascii')
                        console_logger.info(f"Fallback: converted to base58: {cid_string}")
                        image_ref = cid_string
                    except Exception as e2:
                        console_logger.error(f"Fallback also failed: {e2}")
                        return False

            # Get proper author handle
            if author_handle == author_did:  # If we only have DID, try to get handle
                author_handle = self._get_author_handle(author_did)

            # Download the image
            if image_ref:
                console_logger.info(f"🖼️ Found image in post by {author_handle}")
                filename = self._download_image(
                    image_ref,
                    author_did,
                    image_id
                )

                if filename:
                    # Create metadata entry
                    metadata = {
                        'id': image_id,
                        'filename': filename,
                        'author_did': author_did,
                        'author_handle': author_handle,
                        'post_text': post_text,
                        'hashtags': hashtags,
                        'alt_text': alt_text,
                        'timestamp': timestamp,
                        'added_at': datetime.now().isoformat(),
                        'likes': 0,
                        'liked': False
                    }

                    # Add to cache (newest first)
                    self.image_metadata.insert(0, metadata)
                    self.stats['images_found'] += 1

                    # Enforce cache size limit
                    if len(self.image_metadata) > self.max_cache_size:
                        # Remove oldest images (both metadata and file)
                        while len(self.image_metadata) > self.max_cache_size:
                            old_metadata = self.image_metadata.pop()
                            old_file = self.images_dir / old_metadata['filename']
                            if old_file.exists():
                                old_file.unlink()

                    # Save metadata
                    self._save_metadata()

                    console_logger.info(f"✅ Cached image: {filename}")
                    console_logger.info(f"   By: {author_handle}")
                    console_logger.info(f"   Text: {post_text[:50]}...")

                    # Print stats every 10 images
                    if self.stats['images_found'] % 10 == 0:
                        self.print_stats()

                    return True
                else:
                    console_logger.warning(f"❌ Failed to download image for {author_handle}")
            else:
                file_logger.warning(f"❌ Image data missing ref: {image_data}")

        except Exception as e:
            console_logger.error(f"Error adding image to cache: {e}")
            self.stats['errors'] += 1

        return False

    def get_recent_images(self, limit: int = 50):
        """Get recent images for API consumption"""
        return self.image_metadata[:limit]

    def increment_like(self, image_id: str):
        """Increment like count for an image"""
        for image in self.image_metadata:
            if image['id'] == image_id:
                image['likes'] = image.get('likes', 0) + 1
                image['liked'] = True
                self._save_metadata()
                console_logger.info(f"Liked image: {image_id}")
                return True
        return False

    def print_stats(self):
        """Print current statistics"""
        runtime = datetime.now() - datetime.fromisoformat(self.stats['start_time'])
        console_logger.info("📊 STATS: "
                   f"Runtime: {runtime}, "
                   f"Messages: {self.stats['total_messages']}, "
                   f"Posts: {self.stats['posts_processed']}, "
                   f"Images Found: {self.stats['images_found']}, "
                   f"Downloaded: {self.stats['images_downloaded']}, "
                   f"Errors: {self.stats['errors']}, "
                   f"Cache Size: {len(self.image_metadata)}")

class ImageFirehoseListener:
    def __init__(self, cache: ImageFirehoseCache):
        self.cache = cache
        self.hashtag_pattern = re.compile(r'#\w+')
        self.message_count = 0
        self.last_stats_time = time.time()

    def extract_hashtags(self, text):
        """Extract hashtags from post text"""
        if not text:
            return []
        return self.hashtag_pattern.findall(text)


    def on_message_handler(self, message):
        try:
            self.message_count += 1
            self.cache.stats['total_messages'] += 1

            # Print progress every 1000 messages or 30 seconds
            current_time = time.time()
            if self.message_count % 1000 == 0 or current_time - self.last_stats_time > 30:
                console_logger.info(f"📨 Processed {self.message_count} firehose messages")
                self.cache.print_stats()
                self.last_stats_time = current_time

            commit = parse_subscribe_repos_message(message)

            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                file_logger.debug(f"Non-commit message type: {type(commit)}")
                return

            if not commit.blocks:
                file_logger.debug("Commit has no blocks")
                return

            car = CAR.from_bytes(commit.blocks)
            file_logger.debug(f"CAR file parsed with {len(car.blocks)} blocks")

            posts_in_commit = 0
            images_in_commit = 0

            for op in commit.ops:
                if op.action == 'create':
                    record = car.blocks.get(op.cid)
                    if record:
                        # Handle both dict and model record types
                        if isinstance(record, dict):
                            # Raw dict record - check if it's a post
                            if record.get('$type') == 'app.bsky.feed.post':
                                posts_in_commit += 1
                                self.cache.stats['posts_processed'] += 1
                                images_found = self._process_post_dict(record, commit.repo, commit.time)
                                images_in_commit += images_found
                            else:
                                file_logger.debug(f"Non-post dict record type: {record.get('$type', 'unknown')}")

                        # Also handle model instances as before
                        elif isinstance(record, models.AppBskyFeedPost.Record):
                            posts_in_commit += 1
                            self.cache.stats['posts_processed'] += 1
                            images_found = self._process_post_model(record, commit.repo, commit.time)
                            images_in_commit += images_found
                        else:
                            file_logger.debug(f"Unknown record type: {type(record)}")

            if posts_in_commit > 0:
                file_logger.debug(f"Processed {posts_in_commit} posts, found {images_in_commit} images in commit")

        except Exception as e:
            console_logger.error(f"❌ Error processing message: {e}")
            self.cache.stats['errors'] += 1

    def _process_post_dict(self, record_dict, author_did, timestamp):
        """Process a post record that comes as a raw dict"""
        try:
            post_text = record_dict.get('text', '')
            hashtags = self.extract_hashtags(post_text)

            # Get author handle if available (we only have DID from commit)
            author_handle = author_did  # We'll try to get handle later if needed

            embed = record_dict.get('embed')
            images_found = 0

            if embed:
                file_logger.debug(f"Checking embed in dict post by {author_did}")
                images = self._extract_images_from_dict(embed)

                if images:
                    file_logger.debug(f"Found {len(images)} images in dict post")
                    for image in images:
                        if self.cache.add_image(
                            image_data=image,
                            post_text=post_text,
                            author_did=author_did,
                            author_handle=author_handle,
                            timestamp=timestamp,
                            hashtags=hashtags
                        ):
                            images_found += 1
                else:
                    file_logger.debug(f"No images found in dict embed: {embed.get('$type', 'unknown')}")

            return images_found

        except Exception as e:
            console_logger.error(f"❌ Error processing dict post: {e}")
            self.cache.stats['errors'] += 1
            return 0

    def _process_post_model(self, record, author_did, timestamp):
        """Process a post record that comes as a model instance (original method)"""
        try:
            post_text = getattr(record, 'text', '')
            hashtags = self.extract_hashtags(post_text)

            # Get author handle from record if available
            author_handle = getattr(record, 'author', author_did)

            images_found = 0
            if hasattr(record, 'embed') and record.embed:
                file_logger.debug(f"Checking embed in model post by {author_handle}")
                images = self._extract_images_from_model(record.embed)

                if images:
                    file_logger.debug(f"Found {len(images)} images in model post")
                    for image in images:
                        if self.cache.add_image(
                            image_data=image,
                            post_text=post_text,
                            author_did=author_did,
                            author_handle=author_handle,
                            timestamp=timestamp,
                            hashtags=hashtags
                        ):
                            images_found += 1
                else:
                    file_logger.debug(f"No images found in model embed type: {type(record.embed)}")

            return images_found

        except Exception as e:
            console_logger.error(f"❌ Error processing model post: {e}")
            self.cache.stats['errors'] += 1
            return 0

    def _extract_images_from_dict(self, embed_dict):
        """Extract images from embed dict"""
        images = []

        embed_type = embed_dict.get('$type')
        file_logger.debug(f"Extracting images from dict embed type: {embed_type}")

        # Direct images embed
        if embed_type == 'app.bsky.embed.images':
            file_logger.debug("Found app.bsky.embed.images in dict")
            images_data = embed_dict.get('images', [])
            file_logger.debug(f"Found {len(images_data)} images in dict embed")
            images.extend(images_data)

        # Record with media (posts with both text and images)
        elif embed_type == 'app.bsky.embed.recordWithMedia':
            file_logger.debug("Found app.bsky.embed.recordWithMedia in dict")
            media = embed_dict.get('media', {})
            if media.get('$type') == 'app.bsky.embed.images':
                images_data = media.get('images', [])
                file_logger.debug(f"Found {len(images_data)} images in dict recordWithMedia")
                images.extend(images_data)

        return images

    def _extract_images_from_model(self, embed):
        """Extract images from embed model (original method)"""
        images = []

        file_logger.debug(f"Extracting images from model embed type: {type(embed)}")

        # Direct images embed
        if isinstance(embed, models.AppBskyEmbedImages.Main):
            file_logger.debug("Found AppBskyEmbedImages.Main")
            if hasattr(embed, 'images') and embed.images:
                file_logger.debug(f"Found {len(embed.images)} images in model embed")
                images.extend(embed.images)

        # Record with media (posts with both text and images)
        elif isinstance(embed, models.AppBskyEmbedRecordWithMedia.Main):
            file_logger.debug("Found AppBskyEmbedRecordWithMedia.Main")
            if hasattr(embed, 'media') and isinstance(embed.media, models.AppBskyEmbedImages.Main):
                if hasattr(embed.media, 'images') and embed.media.images:
                    file_logger.debug(f"Found {len(embed.media.images)} images in model record with media")
                    images.extend(embed.media.images)

        return images

    def _process_post(self, record, author_did, timestamp):
        """Process post for images"""
        try:
            post_text = getattr(record, 'text', '')
            hashtags = self.extract_hashtags(post_text)

            # Get author handle from record if available
            author_handle = getattr(record, 'author', author_did)

            images_found = 0
            if hasattr(record, 'embed') and record.embed:
                file_logger.debug(f"Checking embed in post by {author_handle}")
                images = self._extract_images(record.embed)

                if images:
                    file_logger.debug(f"Found {len(images)} images in post by {author_handle}")
                    for image in images:
                        if self.cache.add_image(
                            image_data=image,
                            post_text=post_text,
                            author_did=author_did,
                            author_handle=author_handle,
                            timestamp=timestamp,
                            hashtags=hashtags
                        ):
                            images_found += 1
                else:
                    file_logger.debug(f"No images found in embed type: {type(record.embed)}")
            else:
                file_logger.debug(f"Post has no embed or embed is None")

            return images_found

        except Exception as e:
            console_logger.error(f"❌ Error processing post: {e}")
            self.cache.stats['errors'] += 1
            return 0

    def _extract_images(self, embed):
        """Extract images from different embed types"""
        images = []

        file_logger.debug(f"Extracting images from embed type: {type(embed)}")

        # Direct images embed
        if isinstance(embed, models.AppBskyEmbedImages.Main):
            file_logger.debug("Found AppBskyEmbedImages.Main")
            if hasattr(embed, 'images') and embed.images:
                file_logger.debug(f"Found {len(embed.images)} images in embed")
                images.extend(embed.images)

        # Record with media (posts with both text and images)
        elif isinstance(embed, models.AppBskyEmbedRecordWithMedia.Main):
            file_logger.debug("Found AppBskyEmbedRecordWithMedia.Main")
            if hasattr(embed, 'media') and isinstance(embed.media, models.AppBskyEmbedImages.Main):
                if hasattr(embed.media, 'images') and embed.media.images:
                    file_logger.debug(f"Found {len(embed.media.images)} images in record with media")
                    images.extend(embed.media.images)

        return images

# Simple HTTP API
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class CacheAPIHandler(BaseHTTPRequestHandler):
    def __init__(self, cache, *args, **kwargs):
        self.cache = cache
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/images':
            # Return recent images as JSON
            images = self.cache.get_recent_images(50)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(images).encode())

        elif self.path == '/stats':
            # Return stats
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(self.cache.stats).encode())

        elif self.path.startswith('/like/'):
            # Handle like requests
            image_id = self.path.split('/')[-1]
            if self.cache.increment_like(image_id):
                self.send_response(200)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(b'Liked')
            else:
                self.send_response(404)
                self.end_headers()

        elif self.path.startswith('/image/'):
            # Serve actual image files
            filename = self.path.split('/')[-1]
            image_path = self.cache.images_dir / filename
            if image_path.exists():
                self.send_response(200)
                self.send_header('Content-Type', 'image/jpeg')
                self.end_headers()
                with open(image_path, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress HTTP server logs
        pass

def start_api_server(cache, port=8000):
    """Start HTTP API server in a separate thread"""
    handler = lambda *args: CacheAPIHandler(cache, *args)
    server = HTTPServer(('0.0.0.0', port), handler)

    def run_server():
        console_logger.info(f"🚀 Cache API server started on http://localhost:{port}")
        console_logger.info(f"📊 Stats available at http://localhost:{port}/stats")
        server.serve_forever()

    api_thread = threading.Thread(target=run_server, daemon=True)
    api_thread.start()
    return server

def main():
    console_logger.info("🎯 Starting image firehose listener with cache...")
    console_logger.info("📝 Debug logs will be saved to firehose_debug.log")

    # Initialize cache and listener
    cache = ImageFirehoseCache(cache_dir="image_cache", max_cache_size=1000)
    listener = ImageFirehoseListener(cache)

    # Start API server
    start_api_server(cache, port=8000)

    # Start firehose listener
    firehose_client = FirehoseSubscribeReposClient()

    console_logger.info("📡 Firehose: Listening for images")
    console_logger.info("🌐 API: Serving on http://localhost:8000")
    console_logger.info("💾 Cache: Storing images in ./image_cache/")

    try:
        firehose_client.start(listener.on_message_handler)
    except KeyboardInterrupt:
        console_logger.info("🛑 Shutting down...")
        cache.print_stats()
    except Exception as e:
        console_logger.error(f"💥 Fatal error: {e}")
    finally:
        firehose_client.stop()

if __name__ == "__main__":
    
    main()
