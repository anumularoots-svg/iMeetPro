// src/services/whiteboard.js - COMPLETE FIXED SERVICE with correct endpoint mapping
import { API_BASE_URL } from '../utils/constants';

class WhiteboardService {
  constructor() {
    this.baseUrl = `${API_BASE_URL}/api/whiteboard`;
    this.authToken = null;
    this.retryAttempts = 3;
    this.retryDelay = 1000;
    
    console.log('WhiteboardService initialized with baseUrl:', this.baseUrl);
  }

  // Set authentication token
  setAuthToken(token) {
    this.authToken = token;
  }

  // Get auth headers
  getAuthHeaders() {
    const headers = {
      'Content-Type': 'application/json',
    };

    if (this.authToken) {
      headers['Authorization'] = `Bearer ${this.authToken}`;
    } else {
      const token = localStorage.getItem('authToken') || localStorage.getItem('token');
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }
    }

    return headers;
  }

        // Retry mechanism for failed requests
  async retryRequest(requestFn, attempts = this.retryAttempts) {
    for (let i = 0; i < attempts; i++) {
      try {
        return await requestFn();
      } catch (error) {
        console.warn(`Request failed (attempt ${i + 1}/${attempts}):`, error.message);
        
        if (i === attempts - 1) throw error;
        
        // Don't retry for certain error types
        if (error.response?.status === 403 || error.response?.status === 404) {
          throw error;
        }
        
        await new Promise(resolve => setTimeout(resolve, this.retryDelay * (i + 1)));
      }
    }
  }

  // FIXED: Create whiteboard session with correct endpoint
  async createSession(meetingId, userId, participantName = null) {
    try {
      console.log('Creating whiteboard session:', { meetingId, userId, participantName });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/create-session/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId,
            participant_name: participantName || `User_${userId}`
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || `HTTP ${res.status}: Failed to create whiteboard session`);
        }

        return data;
      });

      console.log('✅ Whiteboard session created successfully:', response);
      return {
        success: response.success,
        session: response.session
      };
    } catch (error) {
      console.error('❌ Error creating whiteboard session:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Get whiteboard state with correct endpoint
  async getWhiteboardState(meetingId) {
    try {
      console.log('Getting whiteboard state for meeting:', meetingId);
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/state/${meetingId}/`, {
          method: 'GET',
          headers: this.getAuthHeaders()
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || `HTTP ${res.status}: Failed to get whiteboard state`);
        }

        return data;
      });

      console.log('✅ Whiteboard state retrieved successfully:', response);
      return {
        success: response.success,
        whiteboard: {
          ...response.whiteboard,
          // Enhanced cache-aware state mapping
          can_undo: response.whiteboard.can_undo || false,
          can_redo: response.whiteboard.can_redo || false,
          undo_count: response.whiteboard.undo_count || 0,
          redo_count: response.whiteboard.redo_count || 0,
          can_go_back: response.whiteboard.can_go_back || false,
          can_go_forward: response.whiteboard.can_go_forward || false,
          checkpoints: response.whiteboard.checkpoints || [],
          current_checkpoint: response.whiteboard.current_checkpoint || -1,
          drawings: response.whiteboard.drawings || []
        }
      };
    } catch (error) {
      console.error('❌ Error getting whiteboard state:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Add drawing with correct endpoint
  async addDrawing(meetingId, drawingData, userId) {
    try {
      console.log('Adding drawing:', { meetingId, userId, drawingData });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/add-drawing/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId,
            drawing_id: drawingData.drawing_id || this.generateId(),
            tool_type: drawingData.tool || drawingData.tool_type,
            stroke_color: drawingData.color || drawingData.stroke_color || '#000000',
            stroke_width: drawingData.brushSize || drawingData.stroke_width || 2,
            fill_color: drawingData.fill_color,
            opacity: drawingData.opacity || 1.0,
            drawing_data: {
              type: drawingData.type || 'path',
              points: drawingData.points || [],
              from: drawingData.from,
              to: drawingData.to,
              start: drawingData.start,
              end: drawingData.end,
              ...drawingData
            },
            layer_index: drawingData.layer_index || 0
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || `HTTP ${res.status}: Failed to add drawing`);
        }

        return data;
      });

      console.log('✅ Drawing added successfully:', response);
      return {
        success: response.success,
        drawing: response.drawing,
        broadcast_data: response.broadcast_data
      };
    } catch (error) {
      console.error('❌ Error adding drawing:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

// Enhanced undo with better error handling
async undoAction(meetingId, userId) {
  try {
    console.log('🔄 Performing undo action:', { meetingId, userId });
    
    // CRITICAL: Make sure the endpoint URL is exactly right
    const response = await this.retryRequest(async () => {
      const res = await fetch(`${this.baseUrl}/undo/`, {
        method: 'POST',
        headers: this.getAuthHeaders(),
        body: JSON.stringify({
          meeting_id: meetingId,
          user_id: userId
        })
      });

      if (!res.ok) {
        const errorText = await res.text();
        let errorData;
        try {
          errorData = JSON.parse(errorText);
        } catch {
          errorData = { error: `HTTP ${res.status}: ${errorText}` };
        }
        throw new Error(errorData.error || `HTTP ${res.status}: Failed to undo action`);
      }

      return await res.json();
    });

    console.log('✅ Undo action completed successfully:', response);
    return {
      success: response.success,
      message: response.message,
      undone_action: response.undone_action,
      drawings: response.drawings || [],
      broadcast_data: response.broadcast_data
    };
  } catch (error) {
    console.error('❌ Error in undo action:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

// Enhanced redo with better error handling
async redoAction(meetingId, userId) {
  try {
    console.log('🔄 Performing redo action:', { meetingId, userId });
    
    // CRITICAL: Make sure the endpoint URL is exactly right
    const response = await this.retryRequest(async () => {
      const res = await fetch(`${this.baseUrl}/redo/`, {
        method: 'POST',
        headers: this.getAuthHeaders(),
        body: JSON.stringify({
          meeting_id: meetingId,
          user_id: userId
        })
      });

      if (!res.ok) {
        const errorText = await res.text();
        let errorData;
        try {
          errorData = JSON.parse(errorText);
        } catch {
          errorData = { error: `HTTP ${res.status}: ${errorText}` };
        }
        throw new Error(errorData.error || `HTTP ${res.status}: Failed to redo action`);
      }

      return await res.json();
    });

    console.log('✅ Redo action completed successfully:', response);
    return {
      success: response.success,
      message: response.message,
      redone_action: response.redone_action,
      drawings: response.drawings || [],
      broadcast_data: response.broadcast_data
    };
  } catch (error) {
    console.error('❌ Error in redo action:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

  // FIXED: Clear whiteboard with correct endpoint
  async clearWhiteboard(meetingId, userId) {
    try {
      console.log('🗑️ Clearing whiteboard:', { meetingId, userId });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/clear/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || `HTTP ${res.status}: Failed to clear whiteboard`);
        }

        return data;
      });

      console.log('✅ Whiteboard cleared successfully:', response);
      return {
        success: response.success,
        message: response.message,
        drawings_cleared: response.drawings_cleared,
        broadcast_data: response.broadcast_data
      };
    } catch (error) {
      console.error('❌ Error clearing whiteboard:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Create checkpoint with correct endpoint
  async createCheckpoint(meetingId, userId, name = '') {
    try {
      console.log('Creating checkpoint:', { meetingId, userId, name });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/create-checkpoint/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId,
            name: name || `Checkpoint ${new Date().toLocaleTimeString()}`
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || 'Failed to create checkpoint');
        }

        return data;
      });

      console.log('✅ Checkpoint created successfully:', response);
      return {
        success: response.success,
        message: response.message,
        checkpoint_name: response.checkpoint_name,
        drawings_count: response.drawings_count,
        checkpoint: {
          id: response.checkpoint?.id || this.generateId(),
          name: response.checkpoint_name || name,
          created_at: new Date().toISOString(),
          drawings_count: response.drawings_count || 0
        }
      };
    } catch (error) {
      console.error('❌ Error creating checkpoint:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Navigate to state with correct endpoint
  async navigateToState(meetingId, userId, checkpointId) {
    try {
      console.log('Navigating to checkpoint state:', { meetingId, userId, checkpointId });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/navigate-to-state/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId,
            checkpoint_id: checkpointId
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || 'Failed to navigate to state');
        }

        return data;
      });

      console.log('✅ Navigation completed successfully:', response);
      return {
        success: response.success,
        message: response.message,
        drawings: response.drawings || [],
        checkpoint_name: response.checkpoint_name,
        broadcast_data: response.broadcast_data
      };
    } catch (error) {
      console.error('❌ Error navigating to state:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Get whiteboard history with correct endpoint
  async getHistory(meetingId, userId) {
    try {
      console.log('Getting whiteboard history:', { meetingId, userId });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/history/${meetingId}/`, {
          method: 'GET',
          headers: this.getAuthHeaders()
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || 'Failed to get history');
        }

        return data;
      });

      console.log('✅ History retrieved successfully:', response);
      return {
        success: response.success,
        checkpoints: response.checkpoints || [],
        history: response.history || [],
        total_history_entries: response.total_history_entries || 0,
        total_checkpoints: response.total_checkpoints || 0
      };
    } catch (error) {
      console.error('❌ Error getting history:', error);
      return {
        success: false,
        error: error.message,
        checkpoints: [],
        history: [],
        total_history_entries: 0,
        total_checkpoints: 0
      };
    }
  }

  // FIXED: Update whiteboard settings with correct endpoint
  async updateSettings(meetingId, userId, settings) {
    try {
      console.log('Updating whiteboard settings:', { meetingId, userId, settings });
      
      const response = await this.retryRequest(async () => {
        const res = await fetch(`${this.baseUrl}/update-settings/`, {
          method: 'POST',
          headers: this.getAuthHeaders(),
          body: JSON.stringify({
            meeting_id: meetingId,
            user_id: userId,
            background_color: settings.background_color || settings.backgroundColor,
            grid_enabled: settings.grid_enabled !== undefined ? settings.grid_enabled : settings.gridEnabled,
            high_contrast: settings.high_contrast !== undefined ? settings.high_contrast : settings.highContrast,
            ...settings
          })
        });

        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || 'Failed to update settings');
        }

        return data;
      });

      console.log('✅ Settings updated successfully:', response);
      return {
        success: response.success,
        message: response.message,
        settings: response.settings,
        broadcast_data: response.broadcast_data
      };
    } catch (error) {
      console.error('❌ Error updating settings:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // FIXED: Get cache status with correct endpoint
  async getCacheStatus() {
    try {
      console.log('Getting cache status');
      
      const response = await fetch(`${this.baseUrl}/cache-status/`, {
        method: 'GET',
        headers: this.getAuthHeaders()
      });

      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to get cache status');
      }

      console.log('✅ Cache status retrieved:', data);
      return {
        success: data.success,
        cache_available: data.cache_available,
        redis_info: data.redis_info,
        cache_keys: data.cache_keys
      };
    } catch (error) {
      console.error('❌ Error getting cache status:', error);
      return {
        success: false,
        error: error.message,
        cache_available: false
      };
    }
  }

  // Utility methods
  generateId() {
    return `wb_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  validateDrawingData(drawingData) {
    if (!drawingData || typeof drawingData !== 'object') {
      return false;
    }

    const requiredFields = ['tool'];
    const hasRequiredFields = requiredFields.every(field => 
      drawingData.hasOwnProperty(field) && drawingData[field] !== null && drawingData[field] !== undefined
    );
    
    return hasRequiredFields;
  }

  sanitizeDrawingData(drawingData) {
    const sanitized = { ...drawingData };
    
    // Remove potentially harmful properties
    const dangerousProps = ['script', 'innerHTML', 'outerHTML', '__proto__', 'constructor'];
    dangerousProps.forEach(prop => {
      delete sanitized[prop];
    });
    
    // Ensure numeric values are within reasonable bounds
    if (sanitized.brushSize) {
      sanitized.brushSize = Math.max(1, Math.min(100, Number(sanitized.brushSize)));
    }
    
    if (sanitized.opacity) {
      sanitized.opacity = Math.max(0, Math.min(1, Number(sanitized.opacity)));
    }
    
    // Sanitize coordinate values
    const sanitizeCoordinate = (coord) => {
      if (coord && typeof coord === 'object') {
        return {
          x: Math.max(-10000, Math.min(10000, Number(coord.x) || 0)),
          y: Math.max(-10000, Math.min(10000, Number(coord.y) || 0))
        };
      }
      return coord;
    };
    
    ['from', 'to', 'start', 'end'].forEach(prop => {
      if (sanitized[prop]) {
        sanitized[prop] = sanitizeCoordinate(sanitized[prop]);
      }
    });
    
    // Sanitize color values
    if (sanitized.color && typeof sanitized.color === 'string') {
      if (!/^#[0-9A-Fa-f]{6}$/.test(sanitized.color)) {
        sanitized.color = '#000000';
      }
    }
    
    return sanitized;
  }

  // Batch operations for multiple drawings
  async addMultipleDrawings(meetingId, drawingsArray, userId) {
    const results = [];
    const errors = [];

    for (const drawing of drawingsArray) {
      try {
        const result = await this.addDrawing(meetingId, drawing, userId);
        results.push(result);
        
        if (!result.success) {
          errors.push(result.error);
        }
      } catch (error) {
        errors.push(error.message);
      }
    }

    return {
      success: errors.length === 0,
      results: results,
      errors: errors,
      total_processed: results.length,
      total_errors: errors.length
    };
  }

  // Sync state with backend cache
  async syncState(meetingId, localState, userId) {
    try {
      console.log('Syncing state with backend:', { meetingId, userId });
      
      // Get server state
      const serverStateResult = await this.getWhiteboardState(meetingId);
      
      if (!serverStateResult.success) {
        return { success: false, error: 'Failed to get server state' };
      }
      
      const serverState = serverStateResult.whiteboard;
      
      // Compare and determine if sync is needed
      const needsSync = this.compareStates(localState, serverState);
      
      if (needsSync) {
        console.log('States differ, synchronizing...');
        return {
          success: true,
          serverState: serverState,
          needsUpdate: true,
          syncRequired: true
        };
      }
      
      return {
        success: true,
        serverState: serverState,
        needsUpdate: false,
        syncRequired: false
      };
      
    } catch (error) {
      console.error('❌ Error synchronizing state:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // Compare states to determine sync necessity
  compareStates(localState, serverState) {
    // Compare drawing counts
    const localDrawingCount = localState?.drawings?.length || 0;
    const serverDrawingCount = serverState?.drawings?.length || 0;
    
    if (localDrawingCount !== serverDrawingCount) return true;
    
    // Compare undo/redo counts
    if ((localState?.undoCount || 0) !== (serverState?.undo_count || 0)) return true;
    if ((localState?.redoCount || 0) !== (serverState?.redo_count || 0)) return true;
    
    // Compare checkpoint counts
    const localCheckpoints = localState?.checkpoints?.length || 0;
    const serverCheckpoints = serverState?.checkpoints?.length || 0;
    
    return localCheckpoints !== serverCheckpoints;
  }

  // Performance optimization methods
  debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  }

  throttle(func, limit) {
    let inThrottle;
    return function() {
      const args = arguments;
      const context = this;
      if (!inThrottle) {
        func.apply(context, args);
        inThrottle = true;
        setTimeout(() => inThrottle = false, limit);
      }
    };
  }

  // Legacy compatibility methods
  async saveWhiteboard(meetingId, whiteboardData) {
    const userId = whiteboardData?.user_id || whiteboardData?.userId || 1;
    return await this.createCheckpoint(meetingId, userId, 'Manual save');
  }

  async loadWhiteboard(meetingId) {
    const stateResult = await this.getWhiteboardState(meetingId);
    if (stateResult.success && stateResult.whiteboard) {
      return {
        imageData: stateResult.whiteboard.drawings || [],
        settings: {
          background_color: stateResult.whiteboard.background_color || '#ffffff',
          grid_enabled: stateResult.whiteboard.grid_enabled || false,
          high_contrast: stateResult.whiteboard.high_contrast || false
        },
        history: stateResult.whiteboard.history || { states: [], current_index: -1 },
        navigation: stateResult.whiteboard.navigation || { checkpoints: [], current_index: -1 },
        metadata: {
          created_at: stateResult.whiteboard.created_at,
          updated_at: stateResult.whiteboard.updated_at,
          total_drawings: stateResult.whiteboard.drawings?.length || 0,
          can_undo: stateResult.whiteboard.can_undo || false,
          can_redo: stateResult.whiteboard.can_redo || false,
          undo_count: stateResult.whiteboard.undo_count || 0,
          redo_count: stateResult.whiteboard.redo_count || 0
        }
      };
    }
    return null;
  }
}

// Create singleton instance
const whiteboardService = new WhiteboardService();

// Enhanced API object with all endpoints correctly mapped
export const whiteboardAPI = {
  // Core session methods
  createSession: (meetingId, userId, participantName) => whiteboardService.createSession(meetingId, userId, participantName),
  getWhiteboardState: (meetingId) => whiteboardService.getWhiteboardState(meetingId),
  
  // Drawing operations
  addDrawing: (meetingId, drawingData, userId) => whiteboardService.addDrawing(meetingId, drawingData, userId),
  addValidatedDrawing: (meetingId, drawingData, userId) => {
    if (!whiteboardService.validateDrawingData(drawingData)) {
      return { success: false, error: 'Invalid drawing data provided' };
    }
    const sanitizedData = whiteboardService.sanitizeDrawingData(drawingData);
    return whiteboardService.addDrawing(meetingId, sanitizedData, userId);
  },
  addMultipleDrawings: (meetingId, drawingsArray, userId) => whiteboardService.addMultipleDrawings(meetingId, drawingsArray, userId),
  
  // CRITICAL: Fixed undo/redo operations
  undoAction: (meetingId, userId) => whiteboardService.undoAction(meetingId, userId),
  redoAction: (meetingId, userId) => whiteboardService.redoAction(meetingId, userId),
  clearWhiteboard: (meetingId, userId) => whiteboardService.clearWhiteboard(meetingId, userId),
  
  // Navigation and checkpoints
  createCheckpoint: (meetingId, userId, name) => whiteboardService.createCheckpoint(meetingId, userId, name),
  navigateToState: (meetingId, userId, checkpointId) => whiteboardService.navigateToState(meetingId, userId, checkpointId),
  getHistory: (meetingId, userId) => whiteboardService.getHistory(meetingId, userId),
  
  // Settings and cache
  updateSettings: (meetingId, userId, settings) => whiteboardService.updateSettings(meetingId, userId, settings),
  getCacheStatus: () => whiteboardService.getCacheStatus(),
  syncState: (meetingId, localState, userId) => whiteboardService.syncState(meetingId, localState, userId),
  
  // Legacy compatibility
  saveWhiteboard: (meetingId, data) => whiteboardService.saveWhiteboard(meetingId, data),
  loadWhiteboard: (meetingId) => whiteboardService.loadWhiteboard(meetingId),
  
  // Utility functions
  validateDrawingData: (data) => whiteboardService.validateDrawingData(data),
  sanitizeDrawingData: (data) => whiteboardService.sanitizeDrawingData(data),
  debounce: (func, wait) => whiteboardService.debounce(func, wait),
  throttle: (func, limit) => whiteboardService.throttle(func, limit),
  
  // Performance aliases
  performUndo: (meetingId, userId) => whiteboardService.undoAction(meetingId, userId),
  performRedo: (meetingId, userId) => whiteboardService.redoAction(meetingId, userId),
  performClear: (meetingId, userId) => whiteboardService.clearWhiteboard(meetingId, userId),
};

export { whiteboardService };
export default whiteboardAPI;