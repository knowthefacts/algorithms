from locust import HttpUser, task

class PostAPITest(HttpUser):
    
    @task
    def test_post_endpoint(self):
        """Test your POST endpoint"""
        payload = {
            "key1": "value1",
            "key2": "value2"
            # Replace with your actual payload
        }
        
        headers = {
            'Content-Type': 'application/json'
            # Add auth header if needed:
            # 'Authorization': 'Bearer your-token'
        }
        
        self.client.post("/your-endpoint", 
                        json=payload, 
                        headers=headers)
