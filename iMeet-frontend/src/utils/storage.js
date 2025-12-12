// src/utils/storage.js
class StorageService {
  // Local Storage methods
  setItem(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (error) {
      console.error('Error setting localStorage item:', error);
    }
  }

  getItem(key) {
    try {
      const item = localStorage.getItem(key);
      return item ? JSON.parse(item) : null;
    } catch (error) {
      console.error('Error getting localStorage item:', error);
      return null;
    }
  }

  removeItem(key) {
    try {
      localStorage.removeItem(key);
    } catch (error) {
      console.error('Error removing localStorage item:', error);
    }
  }

  clear() {
    try {
      localStorage.clear();
    } catch (error) {
      console.error('Error clearing localStorage:', error);
    }
  }

  // Session Storage methods
  setSessionItem(key, value) {
    try {
      sessionStorage.setItem(key, JSON.stringify(value));
    } catch (error) {
      console.error('Error setting sessionStorage item:', error);
    }
  }

  getSessionItem(key) {
    try {
      const item = sessionStorage.getItem(key);
      return item ? JSON.parse(item) : null;
    } catch (error) {
      console.error('Error getting sessionStorage item:', error);
      return null;
    }
  }

  removeSessionItem(key) {
    try {
      sessionStorage.removeItem(key);
    } catch (error) {
      console.error('Error removing sessionStorage item:', error);
    }
  }

  // User-specific storage methods
  setUserData(userData) {
    this.setItem('userData', userData);
  }

  getUserData() {
    return this.getItem('userData');
  }

  clearUserData() {
    this.removeItem('userData');
    this.removeItem('accessToken');
    this.removeItem('refreshToken');
  }

  // Meeting-specific storage
  setMeetingSettings(settings) {
    this.setSessionItem('meetingSettings', settings);
  }

  getMeetingSettings() {
    return this.getSessionItem('meetingSettings');
  }
}

export default new StorageService();