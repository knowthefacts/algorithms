import streamlit as st
import json
import base64
import time
import hashlib
from datetime import datetime, timezone
from streamlit.web.server.websocket_headers import _get_websocket_headers

class SecureAuthManager:
    def __init__(self):
        self.user_info = None
        self.token_validation_cache = {}
    
    def get_user_info_with_validation(self):
        """Get user info with token validation and security checks"""
        try:
            headers = _get_websocket_headers()
            user_data = headers.get('x-amzn-oidc-data', '')
            user_identity = headers.get('x-amzn-oidc-identity', '')
            
            if not user_data or not user_identity:
                return {'authenticated': False, 'reason': 'No OIDC data found'}
            
            # Validate and decode token
            token_info = self._validate_token(user_data)
            if not token_info['valid']:
                return {'authenticated': False, 'reason': token_info['reason']}
            
            # Extract user information
            user_info = token_info['payload']
            
            # Security validations
            security_check = self._perform_security_checks(user_info, user_identity)
            if not security_check['valid']:
                return {'authenticated': False, 'reason': security_check['reason']}
            
            # Store validated user info
            self.user_info = {
                'email': user_info.get('email', user_identity),
                'name': user_info.get('name', ''),
                'sub': user_info.get('sub', ''),
                'groups': user_info.get('groups', []),
                'authenticated': True,
                'token_exp': user_info.get('exp', 0),
                'token_iat': user_info.get('iat', 0),
                'session_id': self._generate_session_id(user_info)
            }
            
            # Store in session state with additional security info
            st.session_state.user_info = self.user_info
            st.session_state.last_auth_check = time.time()
            
            return self.user_info
            
        except Exception as e:
            st.error(f"Authentication error: {str(e)}")
            return {'authenticated': False, 'reason': f'Validation error: {str(e)}'}
    
    def _validate_token(self, token_data):
        """Validate JWT token structure and expiration"""
        try:
            # Split JWT token
            parts = token_data.split('.')
            if len(parts) != 3:
                return {'valid': False, 'reason': 'Invalid JWT structure'}
            
            # Decode payload
            payload = parts[1]
            payload += '=' * (4 - len(payload) % 4)
            decoded_payload = base64.b64decode(payload)
            token_payload = json.loads(decoded_payload)
            
            # Check token expiration
            current_time = int(time.time())
            token_exp = token_payload.get('exp', 0)
            
            if token_exp and current_time > token_exp:
                return {'valid': False, 'reason': 'Token expired', 'payload': token_payload}
            
            # Check token issued time (not too old)
            token_iat = token_payload.get('iat', 0)
            if token_iat and (current_time - token_iat) > 86400:  # 24 hours
                return {'valid': False, 'reason': 'Token too old', 'payload': token_payload}
            
            return {'valid': True, 'payload': token_payload}
            
        except Exception as e:
            return {'valid': False, 'reason': f'Token decode error: {str(e)}'}
    
    def _perform_security_checks(self, token_payload, identity):
        """Perform additional security validations"""
        try:
            # Check if email in token matches identity header
            token_email = token_payload.get('email', '')
            if token_email and token_email != identity:
                return {'valid': False, 'reason': 'Email mismatch between token and identity'}
            
            # Check for suspicious token reuse patterns
            token_hash = self._hash_token_content(token_payload)
            current_time = time.time()
            
            # Simple token reuse detection
            if token_hash in self.token_validation_cache:
                last_seen = self.token_validation_cache[token_hash]
                if (current_time - last_seen) < 1:  # Same token used within 1 second
                    return {'valid': False, 'reason': 'Suspicious token reuse detected'}
            
            self.token_validation_cache[token_hash] = current_time
            
            # Clean old cache entries (keep only last 100)
            if len(self.token_validation_cache) > 100:
                old_entries = sorted(self.token_validation_cache.items(), key=lambda x: x[1])
                for old_token, _ in old_entries[:50]:
                    del self.token_validation_cache[old_token]
            
            return {'valid': True}
            
        except Exception as e:
            return {'valid': False, 'reason': f'Security check error: {str(e)}'}
    
    def _hash_token_content(self, token_payload):
        """Create hash of token content for reuse detection"""
        # Use specific fields that shouldn't change for the same user session
        content = f"{token_payload.get('sub', '')}{token_payload.get('iat', '')}{token_payload.get('exp', '')}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _generate_session_id(self, token_payload):
        """Generate unique session ID for tracking"""
        session_data = f"{token_payload.get('sub', '')}{token_payload.get('iat', '')}"
        return hashlib.md5(session_data.encode()).hexdigest()[:12]
    
    def is_authenticated(self):
        """Check authentication with security validations"""
        user_info = self.get_user_info_with_validation()
        return user_info.get('authenticated', False)
    
    def check_token_expiration_warning(self):
        """Check if token is close to expiration and warn user"""
        if not self.user_info:
            return None
        
        token_exp = self.user_info.get('token_exp', 0)
        if not token_exp:
            return None
        
        current_time = int(time.time())
        time_until_exp = token_exp - current_time
        
        # Warn if token expires in less than 5 minutes
        if 0 < time_until_exp < 300:
            return {
                'warning': True,
                'minutes_left': time_until_exp // 60,
                'message': f"Your session will expire in {time_until_exp // 60} minutes"
            }
        
        return None
    
    def get_session_info(self):
        """Get detailed session information for monitoring"""
        if not self.user_info:
            return None
        
        return {
            'session_id': self.user_info.get('session_id'),
            'email': self.user_info.get('email'),
            'token_issued': datetime.fromtimestamp(self.user_info.get('token_iat', 0), tz=timezone.utc),
            'token_expires': datetime.fromtimestamp(self.user_info.get('token_exp', 0), tz=timezone.utc),
            'last_check': datetime.fromtimestamp(st.session_state.get('last_auth_check', 0), tz=timezone.utc)
        }

# Usage in your Streamlit app
def secure_app_layout():
    """Main app with security features"""
    auth_manager = SecureAuthManager()
    
    # Check authentication
    if not auth_manager.is_authenticated():
        st.error("🚫 Authentication failed or session expired")
        st.info("Please refresh the page to re-authenticate")
        st.stop()
    
    # Check for token expiration warning
    exp_warning = auth_manager.check_token_expiration_warning()
    if exp_warning and exp_warning['warning']:
        st.warning(f"⚠️ {exp_warning['message']}")
    
    # Display session info in sidebar (for debugging/monitoring)
    session_info = auth_manager.get_session_info()
    if session_info:
        with st.sidebar:
            with st.expander("Session Info", expanded=False):
                st.write(f"**Session ID:** {session_info['session_id']}")
                st.write(f"**Email:** {session_info['email']}")
                st.write(f"**Token Expires:** {session_info['token_expires'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
                st.write(f"**Last Check:** {session_info['last_check'].strftime('%H:%M:%S')}")
    
    # Main app content
    st.title("Secure Application")
    
    user_info = auth_manager.user_info
    st.write(f"Welcome, {user_info.get('email')}!")
    
    # Display user info in bottom right
    st.markdown(f"""
    <style>
    .user-info {{
        position: fixed;
        bottom: 10px;
        right: 10px;
        background-color: #f0f2f6;
        padding: 5px 10px;
        border-radius: 5px;
        font-size: 12px;
        color: #666;
        z-index: 999;
    }}
    </style>
    <div class="user-info">
        👤 {user_info.get('email')} | Session: {user_info.get('session_id')}
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    secure_app_layout()
