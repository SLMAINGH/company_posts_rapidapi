from flask import Flask, request, jsonify
from queue import Queue
from threading import Thread
import time
import requests
import os

app = Flask(__name__)
request_queue = Queue()

def rate_limited_worker():
    while True:
        if not request_queue.empty():
            job = request_queue.get()
            try:
                # 1. CALL THE LINKEDIN API
                print(f"Processing job for: {job.get('api_url')}")
                response = requests.get(
                    job['api_url'],
                    headers=job.get('headers', {}),
                    timeout=10
                )
                
                # Handle Rate Limiting
                if response.status_code == 429:
                    print(f"⚠ Rate limited, requeueing...")
                    job['retries'] = job.get('retries', 0) + 1
                    if job['retries'] < 3:
                        request_queue.put(job)
                        time.sleep(5)
                    continue
                
                # 2. PARSE AND FORMAT POSTS
                formatted_posts_string = ""
                try:
                    api_data = response.json()
                    
                    # --- FIX IS HERE ---
                    # Based on your JSON, 'data' is a list, not a dictionary containing another data key
                    raw_posts = api_data.get('data', [])
                    
                    lines = []
                    if isinstance(raw_posts, list):
                        for post in raw_posts:
                            # Safely get fields
                            created_at = post.get('created_at', 'N/A')
                            text = post.get('text', '')
                            
                            # Get reaction count safely
                            activity = post.get('activity', {})
                            reactions = activity.get('num_likes', 0)
                            
                            # Format the line exactly as requested
                            line = f"[created_at={created_at}][reactions={reactions}][text={text}]"
                            lines.append(line)
                        
                        # Join lines with newlines and wrap in curly braces
                        if lines:
                            formatted_posts_string = "{\n" + "\n".join(lines) + "\n}"
                        else:
                            formatted_posts_string = "{ No posts found in list }"
                    else:
                        formatted_posts_string = "{ Unexpected data format }"
                        print(f"Expected list for 'data', got {type(raw_posts)}")
                        
                except Exception as parse_error:
                    print(f"Error parsing posts: {parse_error}")
                    formatted_posts_string = "{ Error parsing posts }"

                # 3. SEND TO WEBHOOK
                if job.get('webhook_url'):
                    print(f"Sending to webhook: {job['webhook_url']}")
                    try:
                        webhook_response = requests.post(
                            job['webhook_url'],
                            json={
                                'status': response.status_code,
                                'company_name': job.get('company_name'),
                                'org_id': job.get('org_id'),
                                'urn': job.get('urn'),
                                # This is the new formatted string field
                                'posts': formatted_posts_string 
                            },
                            timeout=5
                        )
                        print(f"Webhook response: {webhook_response.status_code}")
                    except Exception as webhook_e:
                        print(f"Webhook failed: {webhook_e}")
                        pass
                
                print(f"✓ Success: {response.status_code}")
            except Exception as e:
                print(f"✗ Error: {str(e)}")
            
            time.sleep(3)
        else:
            time.sleep(0.1)

@app.route('/process', methods=['POST'])
def process():
    try:
        data = request.json
        
        # --- FIX FOR URN/ORG_ID EXTRACTION ---
        # 1. Try to get URN from root
        urn_val = data.get('urn')
        # 2. If not at root, try to get it from headers (based on your input JSON)
        if not urn_val and 'headers' in data:
            urn_val = data['headers'].get('urn')
            
        request_queue.put({
            'api_url': data.get('target_api'),
            'headers': data.get('headers', {}),
            'webhook_url': data.get('callback_webhook'),
            'company_name': data.get('company_name'),
            'org_id': data.get('org_id'),
            'urn': urn_val,  # Use the extracted value
            'retries': 0
        })
        return jsonify({'status': 'queued', 'queue_size': request_queue.qsize()}), 202
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'queue_size': request_queue.qsize()})

if __name__ == '__main__':
    worker = Thread(target=rate_limited_worker, daemon=True)
    worker.start()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
