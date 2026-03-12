import React, { useState } from 'react';
import './LoginPage.css';
import { FaUserEdit, FaLock, FaEye, FaEyeSlash } from "react-icons/fa";
import { useNavigate } from 'react-router-dom';
import { Link } from 'react-router-dom';
import travBridgeIcon from '../Assets/Login/Travbridge_logo.png';
import OpenEye from '../Assets/Login/Open_eye.png';
import ClosedEye from '../Assets/Login/Closed_eye.png';
import GoogleIcon from '../Assets/Login/Google_icon.png';

// ✅ Valid users with their respective passwords (emails stored in lowercase)
const VALID_USERS = {
  'dashboard@travbridge.com': 'travbridgestats123$',
  'abraham.alapatt@travbridge.com': 'travbridgestats123$',
  'girish.parmar@thomascook.in': 'Girish@TC2025',
  'jayant.katti@thomascook.in': 'Jayant@TC2025',
  'pritamkumar.das@thomascook.in': 'Pritam@TC2025',
  'sagar.sawant2@thomascook.in': 'Sagar@TC2025'
};

const LoginForm = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [emailError, setEmailError] = useState('');
  const [passwordError, setPasswordError] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const navigate = useNavigate();

  const handleEmailChange = (e) => {
    setEmail(e.target.value);
    setEmailError('');
  };

  const handlePasswordChange = (e) => {
    setPassword(e.target.value);
    setPasswordError('');
  };

  const handleSubmit = (event) => {
    event.preventDefault();

    // Clear previous errors
    setEmailError('');
    setPasswordError('');

    // Basic validation
    if (!email) {
      setEmailError('Please enter your email');
      return;
    }

    if (!password) {
      setPasswordError('Please enter your password');
      return;
    }

    // ✅ Normalize email for case-insensitive check
    const normalizedEmail = email.trim().toLowerCase();
    
    // ✅ Check if email exists in VALID_USERS
    if (VALID_USERS.hasOwnProperty(normalizedEmail)) {
      // ✅ Check if password matches for this email
      if (VALID_USERS[normalizedEmail] === password) {
        console.log('Login successful!');
        localStorage.setItem('isLoggedIn', 'true');
        localStorage.setItem('userEmail', email); // Optional: store user email
        navigate('/home');
      } else {
        setPasswordError('Invalid password');
      }
    } else {
      setEmailError('Invalid email address');
    }
  };

  return (
    <div className="loginform-outerbg">
      <div className="loginform-overlay">
        <div className="loginform-container">
          <form onSubmit={handleSubmit}>
            <h3>Welcome back</h3>

            <div className="input-group">
              <input
                type="email"
                required
                placeholder="Email"
                value={email}
                onChange={handleEmailChange}
              />
              {emailError && <div className="error-message">{emailError}</div>}
            </div>

            <div className="input-group">
              <div style={{ position: 'relative' }}>
                <input
                  type={showPassword ? "text" : "password"}
                  required
                  placeholder="Password"
                  value={password}
                  onChange={handlePasswordChange}
                  style={{ paddingRight: '40px' }}
                />
                <span
                  onClick={() => setShowPassword(!showPassword)}
                  style={{
                    position: 'absolute',
                    right: '10px',
                    top: '50%',
                    transform: 'translateY(-50%)',
                    cursor: 'pointer',
                    fontSize: '18px'
                  }}
                >
                  {showPassword ? <FaEyeSlash /> : <FaEye />}
                </span>
              </div>
              {passwordError && <div className="error-message">{passwordError}</div>}
            </div>

            <button type="submit" className="continue-button">
              <p className="login">Log in</p>
            </button>
          </form>
        </div>
      </div>
    </div>
  );
};

export default LoginForm;