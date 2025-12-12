import { authAPI } from './api';
import { TOKEN_KEY, REFRESH_TOKEN_KEY, USER_KEY } from '@/utils/constants';

class AuthService {
  constructor() {
    this.user = null;
    this.token = null;
    this.refreshToken = null;
    this.init();
  }

  init() {
    // Load user data from localStorage on initialization
    const token = localStorage.getItem(TOKEN_KEY);
    const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY);
    const user = localStorage.getItem(USER_KEY);

    if (token && refreshToken && user) {
      this.token = token;
      this.refreshToken = refreshToken;
      this.user = JSON.parse(user);
    }

    // ✅ NEW: Listen for storage events from other tabs/windows
    this.setupStorageListener();
  }

  // ✅ NEW: Setup cross-tab synchronization
  setupStorageListener() {
    if (typeof window !== 'undefined') {
      window.addEventListener('storage', (event) => {
        // Sync user data across tabs
        if (event.key === USER_KEY && event.newValue) {
          const updatedUser = JSON.parse(event.newValue);
          this.user = updatedUser;
          console.log('🔄 AuthService: User synced from another tab');
          
          // Dispatch event for components to update
          window.dispatchEvent(new CustomEvent('userUpdated', {
            detail: { user: updatedUser }
          }));
        }
        
        // Handle logout in other tabs
        if (event.key === TOKEN_KEY && !event.newValue) {
          this.clearAuthData();
          window.location.href = '/auth';
        }
      });
    }
  }

  // Login user
  async login(credentials) {
    try {
      const response = await authAPI.login(credentials);
      const { access, refresh, user } = response;

      // Store tokens and user data
      this.token = access;
      this.refreshToken = refresh;
      this.user = user;

      // Save to localStorage
      localStorage.setItem(TOKEN_KEY, access);
      localStorage.setItem(REFRESH_TOKEN_KEY, refresh);
      localStorage.setItem(USER_KEY, JSON.stringify(user));

      console.log('✅ User logged in:', user.email);

      return { success: true, user };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Register new user
  async register(userData) {
    try {
      console.log('📤 Registering user...');
      const response = await authAPI.register(userData);
      
      // Auto-login after successful registration if tokens are returned
      if (response.access && response.refresh) {
        const { access, refresh, user } = response;
        
        this.token = access;
        this.refreshToken = refresh;
        this.user = user;

        localStorage.setItem(TOKEN_KEY, access);
        localStorage.setItem(REFRESH_TOKEN_KEY, refresh);
        localStorage.setItem(USER_KEY, JSON.stringify(user));

        console.log('✅ User registered and auto-logged in');
        return { success: true, user, autoLogin: true };
      }

      // If email verification is required
      console.log('📧 Email verification required');
      return { success: true, requiresVerification: true };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Logout user
  async logout() {
    try {
      if (this.refreshToken) {
        await authAPI.logout();
      }
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      // Clear local data regardless of API call success
      this.clearAuthData();
      console.log('👋 User logged out');
    }
  }

  // Clear authentication data
  clearAuthData() {
    this.user = null;
    this.token = null;
    this.refreshToken = null;
    
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }

  // Forgot password
  async forgotPassword(email) {
    try {
      await authAPI.forgotPassword(email);
      return { success: true };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Reset password
  async resetPassword(token, password) {
    try {
      await authAPI.resetPassword(token, password);
      return { success: true };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Verify email
  async verifyEmail(token) {
    try {
      const response = await authAPI.verifyEmail(token);
      
      // Auto-login after successful verification if tokens are returned
      if (response.access && response.refresh) {
        const { access, refresh, user } = response;
        
        this.token = access;
        this.refreshToken = refresh;
        this.user = user;

        localStorage.setItem(TOKEN_KEY, access);
        localStorage.setItem(REFRESH_TOKEN_KEY, refresh);
        localStorage.setItem(USER_KEY, JSON.stringify(user));

        return { success: true, user, autoLogin: true };
      }

      return { success: true };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Get current user profile
  async getProfile() {
    try {
      const user = await authAPI.getProfile();
      this.user = user;
      localStorage.setItem(USER_KEY, JSON.stringify(user));
      
      // ✅ NEW: Dispatch event for immediate UI updates
      window.dispatchEvent(new CustomEvent('userUpdated', {
        detail: { user }
      }));
      
      return user;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // ✅ UPDATED: Update user profile with enhanced sync
  async updateProfile(data) {
    try {
      console.log('💾 AuthService: Updating profile...', {
        hasProfilePicture: !!data.profile_picture,
        pictureLength: data.profile_picture?.length
      });

      const response = await authAPI.updateProfile(data);
      
      // Handle different response formats from backend
      const updatedUser = response.user || response;
      
      // Merge updated data with existing user data
      this.user = {
        ...this.user,
        ...updatedUser,
        // Ensure profile picture is properly set
        profile_picture: updatedUser.profile_picture || data.profile_picture,
      };

      // Save to localStorage
      localStorage.setItem(USER_KEY, JSON.stringify(this.user));
      
      console.log('✅ AuthService: Profile updated successfully');
      console.log('📸 Profile picture:', this.user.profile_picture?.substring(0, 100));

      // ✅ NEW: Dispatch event for immediate UI updates across all components
      window.dispatchEvent(new CustomEvent('userUpdated', {
        detail: { user: this.user }
      }));

      // ✅ NEW: Dispatch specific profile picture event if picture was updated
      if (data.profile_picture) {
        window.dispatchEvent(new CustomEvent('profilePictureUpdated', {
          detail: { 
            profile_picture: this.user.profile_picture,
            user: this.user 
          }
        }));
        console.log('📤 Profile picture update event dispatched');
      }

      return this.user;
    } catch (error) {
      console.error('❌ AuthService: Profile update failed:', error);
      throw this.handleError(error);
    }
  }

  // ✅ NEW: Update profile picture only (optimized for immediate saves)
  async updateProfilePicture(profilePicture) {
    try {
      console.log('📸 AuthService: Updating profile picture only...');

      // Get user ID from various possible field names
      const userId = this.user?.id || this.user?.Id || this.user?.ID || this.user?.user_id;
      
      if (!userId) {
        throw new Error('User ID not found');
      }

      const response = await authAPI.updateProfile({
        id: userId,
        profile_picture: profilePicture
      });

      // Handle different response formats
      const updatedUser = response.user || response;

      // Update user object
      this.user = {
        ...this.user,
        profile_picture: updatedUser.profile_picture || profilePicture,
      };

      // Save to localStorage
      localStorage.setItem(USER_KEY, JSON.stringify(this.user));

      // Dispatch events
      window.dispatchEvent(new CustomEvent('userUpdated', {
        detail: { user: this.user }
      }));

      window.dispatchEvent(new CustomEvent('profilePictureUpdated', {
        detail: { 
          profile_picture: this.user.profile_picture,
          user: this.user 
        }
      }));

      console.log('✅ Profile picture updated and synced across app');

      return this.user;
    } catch (error) {
      console.error('❌ Profile picture update failed:', error);
      throw this.handleError(error);
    }
  }

  // Change password
  async changePassword(data) {
    try {
      await authAPI.changePassword(data);
      return { success: true };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // Check if user is authenticated
  isAuthenticated() {
    return !!(this.token && this.refreshToken && this.user);
  }

  // Get current user
  getCurrentUser() {
    return this.user;
  }

  // Get authentication token
  getToken() {
    return this.token;
  }

  // Check if user has specific role
  hasRole(role) {
    return this.user?.role === role;
  }

  // Check if user has permission
  hasPermission(permission) {
    return this.user?.permissions?.includes(permission);
  }

  // Get user's full name
  getUserDisplayName() {
    if (!this.user) return 'Anonymous';
    return this.user.full_name || this.user.name || this.user.email || 'User';
  }

  // ✅ UPDATED: Get user's avatar URL with fallback support
  getUserAvatar() {
    // Support multiple field names from backend
    return this.user?.profile_picture || 
           this.user?.profile_photo || 
           this.user?.avatar || 
           null;
  }

  // ✅ NEW: Get user's initials for avatar fallback
  getUserInitials() {
    if (!this.user) return 'U';
    const name = this.user.full_name || this.user.name || this.user.email || 'User';
    return name.charAt(0).toUpperCase();
  }

  // Refresh authentication token
  async refreshAuthToken() {
    try {
      if (!this.refreshToken) {
        throw new Error('No refresh token available');
      }

      const response = await authAPI.refreshToken(this.refreshToken);
      const { access } = response;

      this.token = access;
      localStorage.setItem(TOKEN_KEY, access);

      return access;
    } catch (error) {
      this.clearAuthData();
      throw this.handleError(error);
    }
  }

  // Handle API errors
  handleError(error) {
    const message = error.response?.data?.message || 
                   error.response?.data?.Message ||
                   error.response?.data?.Error ||
                   error.response?.data?.detail || 
                   error.message || 
                   'An unexpected error occurred';
    
    const statusCode = error.response?.status;
    
    return {
      message,
      statusCode,
      errors: error.response?.data?.errors || null
    };
  }

  // Initialize password strength checker
  checkPasswordStrength(password) {
    const checks = {
      length: password.length >= 8,
      uppercase: /[A-Z]/.test(password),
      lowercase: /[a-z]/.test(password),
      numbers: /\d/.test(password),
      symbols: /[^A-Za-z0-9]/.test(password)
    };

    const passedChecks = Object.values(checks).filter(Boolean).length;
    let strength = 'weak';
    
    if (passedChecks >= 4) strength = 'strong';
    else if (passedChecks >= 3) strength = 'medium';

    return {
      strength,
      checks,
      score: passedChecks
    };
  }

  // Validate registration data
  validateRegistrationData(data) {
    const errors = {};

    // Full name validation
    if (!data.full_name || data.full_name.trim().length < 2) {
      errors.full_name = 'Full name must be at least 2 characters long';
    }

    // Email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!data.email || !emailRegex.test(data.email)) {
      errors.email = 'Please enter a valid email address';
    }

    // Password validation
    const passwordCheck = this.checkPasswordStrength(data.password);
    if (passwordCheck.strength === 'weak') {
      errors.password = 'Password must be at least 8 characters with uppercase, lowercase, and numbers';
    }

    // Confirm password validation
    if (data.password !== data.confirm_password) {
      errors.confirm_password = 'Passwords do not match';
    }

    // Phone number validation (if provided)
    if (data.phone_number && data.phone_number.trim()) {
      const phoneRegex = /^\+?[\d\s\-\(\)]{10,}$/;
      if (!phoneRegex.test(data.phone_number)) {
        errors.phone_number = 'Please enter a valid phone number';
      }
    }

    return {
      isValid: Object.keys(errors).length === 0,
      errors
    };
  }

  // ✅ NEW: Subscribe to user updates (for React components)
  subscribeToUserUpdates(callback) {
    const handler = (event) => {
      if (event.detail?.user) {
        callback(event.detail.user);
      }
    };

    window.addEventListener('userUpdated', handler);

    // Return unsubscribe function
    return () => {
      window.removeEventListener('userUpdated', handler);
    };
  }

  // ✅ NEW: Subscribe to profile picture updates specifically
  subscribeToProfilePictureUpdates(callback) {
    const handler = (event) => {
      if (event.detail?.profile_picture) {
        callback(event.detail.profile_picture, event.detail.user);
      }
    };

    window.addEventListener('profilePictureUpdated', handler);

    // Return unsubscribe function
    return () => {
      window.removeEventListener('profilePictureUpdated', handler);
    };
  }

  // ✅ NEW: Force refresh user data from backend
  async refreshUserData() {
    try {
      console.log('🔄 Refreshing user data from backend...');
      const updatedUser = await this.getProfile();
      console.log('✅ User data refreshed');
      return updatedUser;
    } catch (error) {
      console.error('❌ Failed to refresh user data:', error);
      throw this.handleError(error);
    }
  }

  // ✅ NEW: Get user profile picture URL (handles S3 URLs and base64)
  getProfilePictureUrl() {
    const picture = this.getUserAvatar();
    
    if (!picture) return null;

    // If it's already a full URL (S3, CDN, etc.)
    if (picture.startsWith('http://') || picture.startsWith('https://')) {
      return picture;
    }

    // If it's a base64 data URL
    if (picture.startsWith('data:image')) {
      return picture;
    }

    // If it's a relative path, construct full URL
    // Adjust this based on your backend configuration
    const backendUrl = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8111';
    return `${backendUrl}/api/profile-photo/${picture}`;
  }

  // ✅ NEW: Check if profile picture is loaded
  hasProfilePicture() {
    const avatar = this.getUserAvatar();
    return !!(avatar && avatar.length > 0);
  }

  // ✅ NEW: Update multiple user fields at once
  async updateUserFields(fields) {
    try {
      const userId = this.user?.id || this.user?.Id || this.user?.ID || this.user?.user_id;
      
      const response = await authAPI.updateProfile({
        id: userId,
        ...fields
      });

      const updatedUser = response.user || response;

      this.user = {
        ...this.user,
        ...updatedUser
      };

      localStorage.setItem(USER_KEY, JSON.stringify(this.user));

      window.dispatchEvent(new CustomEvent('userUpdated', {
        detail: { user: this.user }
      }));

      return this.user;
    } catch (error) {
      throw this.handleError(error);
    }
  }
}

// Create and export singleton instance
const authService = new AuthService();

// ✅ NEW: Export both the instance and the class
export default authService;
export { AuthService };