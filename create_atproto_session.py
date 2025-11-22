#!/usr/bin/env python3
"""
Script to create session.txt file for Bluesky authentication
Run this once to generate your session file
If your session is invalid, delete session.txt and run again
requires you to have an app password, NOT your Bluesky account password
"""

import os
from atproto import Client

def create_session_file():
    print("Bluesky Session File Creator")
    print("=" * 40)

    # Get credentials
    handle = input("Enter your Bluesky handle (e.g., user.bsky.social): ").strip()
    app_password = input("Enter your app password: ").strip()

    if not handle or not app_password:
        print("Error: Both handle and app password are required!")
        return

    # Create client and login
    client = Client()

    try:
        print("Logging in...")
        client.login(handle, app_password)

        # Export session string
        session_string = client.export_session_string()

        # Save to file
        with open('session.txt', 'w') as f:
            f.write(session_string)

        print("✅ Success! session.txt has been created.")
        print(f"Session string length: {len(session_string)} characters")

        # Verify it works
        print("Verifying session...")
        client2 = Client()
        client2.login(session_string=session_string)
        profile = client2.get_profile()
        print(f"✅ Verified! Logged in as: {profile.handle}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_session_file()
