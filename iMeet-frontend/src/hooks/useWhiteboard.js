// Enhanced useWhiteboard.js - COMPLETE FIXED VERSION
import { useState, useRef, useCallback, useEffect } from 'react';
import { whiteboardAPI } from '../services/whiteboard';

export const useWhiteboard = (meetingId) => {
  // Drawing state
  const [tool, setTool] = useState('pen');
  const [color, setColor] = useState('#000000');
  const [brushSize, setBrushSize] = useState(3);
  const [isDrawing, setIsDrawing] = useState(false);
  const [history, setHistory] = useState([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [participants, setParticipants] = useState(new Map());
  const [highContrast, setHighContrast] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [textSize, setTextSize] = useState(16);
  
  const [textFormat, setTextFormat] = useState({
    fontSize: 20,
    bold: false,
    italic: false,
    underline: false,
    strikethrough: false, 
    fontFamily: 'Arial',
  });

  const canvasRef = useRef(null);
  const contextRef = useRef(null);
  const redrawQueueRef = useRef(null);
  
  const [drawingCommands, setDrawingCommands] = useState([]);
  const [snapshots, setSnapshots] = useState([]);
  
  const [backendState, setBackendState] = useState({
    canUndo: false,
    canRedo: false,
    canGoBack: false,
    canGoForward: false,
    undoCount: 0,
    redoCount: 0,
    checkpoints: [],
    currentCheckpoint: -1
  });
  
  const [currentUser, setCurrentUser] = useState(() => {
    try {
      const userData = localStorage.getItem('user');
      return userData ? JSON.parse(userData) : { id: null };
    } catch {
      return { id: null };
    }
  });

  console.log('useWhiteboard initialized:', { meetingId, userId: currentUser.id });

  // ✅ FIXED: Set canvas reference properly
  const setCanvasRef = useCallback((ref) => {
    try {
      // Store the ref to the WhiteboardCanvas component
      canvasRef.current = ref?.current || ref;
      
      if (!canvasRef.current) {
        console.warn('⚠ Invalid canvas reference');
        return;
      }
      
      console.log('✅ Canvas reference stored successfully');
    } catch (error) {
      console.error('❌ Error setting canvas reference:', error);
    }
  }, []);

  const syncWithBackend = useCallback(async () => {
    if (!meetingId) {
      console.warn('⚠️ Cannot sync: no meetingId');
      return;
    }
    
    try {
      console.log('🔄 Syncing with backend for meeting:', meetingId);
      const response = await whiteboardAPI.getWhiteboardState(meetingId);
      
      if (response.success && response.whiteboard) {
        console.log('✅ Backend sync successful:', response.whiteboard);
        
        setBackendState({
          canUndo: response.whiteboard.can_undo || false,
          canRedo: response.whiteboard.can_redo || false,
          canGoBack: response.whiteboard.can_go_back || false,
          canGoForward: response.whiteboard.can_go_forward || false,
          undoCount: response.whiteboard.undo_count || 0,
          redoCount: response.whiteboard.redo_count || 0,
          checkpoints: response.whiteboard.checkpoints || [],
          currentCheckpoint: response.whiteboard.current_checkpoint || -1
        });
        
        const drawings = response.whiteboard.drawings || [];
        setHistory(prev => {
          if (JSON.stringify(prev) !== JSON.stringify(drawings)) {
            console.log('📝 Updating local history from backend:', drawings.length, 'drawings');
            return drawings;
          }
          return prev;
        });
      } else {
        console.error('❌ Backend sync failed:', response.error);
      }
    } catch (error) {
      console.error('❌ Failed to sync with backend:', error);
    }
  }, [meetingId]);

  // ✅ CRITICAL FIX: Proper canvas ready check with getCanvas()
  const ensureCanvasReady = async (maxAttempts = 10) => {
    let attempt = 0;
    while (attempt < maxAttempts) {
      // ✅ FIXED: Get actual canvas via imperative handle
      const canvas = canvasRef.current?.getCanvas?.();
      
      if (canvas && typeof canvas.getContext === 'function') {
        if (canvas.width > 0 && canvas.height > 0) {
          console.log('✅ Canvas is ready');
          return true;
        }
      }
      
      await new Promise((r) => setTimeout(r, 100));
      attempt++;
    }
    console.warn("⚠️ Canvas not ready after retries");
    return false;
  };

  const redrawCanvasFromData = useCallback(async (drawings) => {
    const isReady = await ensureCanvasReady();
    if (!isReady || !canvasRef.current || !Array.isArray(drawings)) {
      console.warn('⚠️ Cannot redraw: canvas not ready or invalid drawings');
      return;
    }

    // ✅ FIXED: Get actual canvas element
    const canvas = canvasRef.current.getCanvas();
    if (!canvas) {
      console.warn('⚠️ Cannot get canvas element');
      return;
    }
    
    const ctx = canvas.getContext('2d');
    
    try {
      console.log('🎨 Redrawing canvas with', drawings.length, 'drawings');
      
      await new Promise(resolve => requestAnimationFrame(resolve));
      
      ctx.save();
      ctx.globalCompositeOperation = 'source-over';
      ctx.globalAlpha = 1.0;
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      
      for (let i = 0; i < drawings.length; i++) {
        const drawing = drawings[i];
        
        if (!drawing.drawing_data || !drawing.tool_type) {
          console.warn('⚠️ Skipping invalid drawing:', drawing);
          continue;
        }
        
        const data = drawing.drawing_data;
        
        ctx.strokeStyle = drawing.stroke_color || '#000000';
        ctx.lineWidth = drawing.stroke_width || 2;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.globalAlpha = drawing.opacity || 1.0;
        
        if (drawing.tool_type === 'eraser') {
          ctx.globalCompositeOperation = 'destination-out';
        } else {
          ctx.globalCompositeOperation = 'source-over';
        }
        
        if (data.type === 'path' && Array.isArray(data.points) && data.points.length > 0) {
          ctx.beginPath();
          ctx.moveTo(data.points[0].x, data.points[0].y);
          
          for (let j = 1; j < data.points.length; j++) {
            ctx.lineTo(data.points[j].x, data.points[j].y);
          }
          
          ctx.stroke();
        } else if (data.type === 'shape' && data.start && data.end) {
          drawShape(ctx, drawing.tool_type, data.start, data.end, drawing.stroke_color, drawing.stroke_width);
        } else if (data.type === 'text' && data.text && data.position) {
          ctx.save();
          ctx.font = data.font || `${drawing.stroke_width * 4 || 16}px Arial`;
          ctx.fillStyle = drawing.stroke_color || '#000000';
          ctx.textBaseline = 'top';
          ctx.globalCompositeOperation = 'source-over';
          
          const lines = data.text.split('\n');
          const lineHeight = data.lineHeight || (data.fontSize || 16) * 1.2;
          
          lines.forEach((line, index) => {
            const yPosition = data.position.y + (index * lineHeight);
            ctx.fillText(line, data.position.x, yPosition);
            
            const textWidth = ctx.measureText(line).width;
            
            if (data.underline) {
              const fontSize = data.fontSize || 16;
              const lineY = yPosition + fontSize + 2;
              ctx.beginPath();
              ctx.moveTo(data.position.x, lineY);
              ctx.lineTo(data.position.x + textWidth, lineY);
              ctx.lineWidth = Math.max(1, fontSize / 16);
              ctx.strokeStyle = drawing.stroke_color || '#000000';
              ctx.stroke();
            }
            
            if (data.strikethrough) {
              const fontSize = data.fontSize || 16;
              const lineY = yPosition + (fontSize / 2);
              ctx.beginPath();
              ctx.moveTo(data.position.x, lineY);
              ctx.lineTo(data.position.x + textWidth, lineY);
              ctx.lineWidth = Math.max(1, fontSize / 16);
              ctx.strokeStyle = drawing.stroke_color || '#000000';
              ctx.stroke();
            }
          });
          
          ctx.restore();
        }
      }
      
      ctx.restore();
      console.log('✅ Canvas redraw complete');
      
    } catch (error) {
      console.error('❌ Redraw error:', error);
    }
  }, []);

  const drawShape = (ctx, shapeType, start, end, strokeColor, lineWidth) => {
    if (!ctx || !start || !end) return;

    ctx.beginPath();
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = lineWidth;

    switch (shapeType) {
      case 'line':
        ctx.moveTo(start.x, start.y);
        ctx.lineTo(end.x, end.y);
        break;

      case 'rectangle':
        ctx.rect(start.x, start.y, end.x - start.x, end.y - start.y);
        break;

      case 'circle': {
        const radius = Math.sqrt(Math.pow(end.x - start.x, 2) + Math.pow(end.y - start.y, 2));
        ctx.arc(start.x, start.y, radius, 0, 2 * Math.PI);
        break;
      }

      case 'triangle':
        ctx.moveTo(start.x + (end.x - start.x) / 2, start.y);
        ctx.lineTo(start.x, end.y);
        ctx.lineTo(end.x, end.y);
        ctx.closePath();
        break;

      case 'arrow': {
        ctx.moveTo(start.x, start.y);
        ctx.lineTo(end.x, end.y);
        const angle = Math.atan2(end.y - start.y, end.x - start.x);
        const headLength = 20;
        const headAngle = Math.PI / 6;
        ctx.moveTo(end.x, end.y);
        ctx.lineTo(
          end.x - headLength * Math.cos(angle - headAngle),
          end.y - headLength * Math.sin(angle - headAngle)
        );
        ctx.moveTo(end.x, end.y);
        ctx.lineTo(
          end.x - headLength * Math.cos(angle + headAngle),
          end.y - headLength * Math.sin(angle + headAngle)
        );
        break;
      }
    }

    ctx.stroke();
  };

  // ✅ CRITICAL FIX: Undo with proper canvas access
  const undo = useCallback(async () => {
    if (!meetingId || !currentUser.id || !backendState.canUndo || isLoading) {
      console.warn('⚠️ Cannot undo:', { 
        meetingId, 
        userId: currentUser.id, 
        canUndo: backendState.canUndo, 
        isLoading 
      });
      return false;
    }
    
    setIsLoading(true);
    
    try {
      console.log('🔄 Starting undo operation...');
      
      // Step 1: Call backend to perform undo
      const response = await whiteboardAPI.undoAction(meetingId, currentUser.id);
      
      if (!response.success) {
        console.error('❌ Undo failed:', response.error);
        return false;
      }
      
      console.log('✅ Backend undo successful, drawings count:', response.drawings?.length || 0);
      
      // Step 2: Wait for canvas to be ready
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Step 3: CRITICAL - Get actual canvas and clear it
      const canvas = canvasRef.current?.getCanvas?.();
      if (canvas) {
        const ctx = canvas.getContext('2d');
        
        console.log('🎨 Clearing canvas completely...');
        
        // Save and reset context
        ctx.save();
        ctx.setTransform(1, 0, 0, 1, 0, 0);
        ctx.globalCompositeOperation = 'source-over';
        ctx.globalAlpha = 1.0;
        
        // Clear everything
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        
        // Fill with white background
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        
        ctx.restore();
        
        console.log('✅ Canvas cleared');
      }
      
      // Step 4: Wait for next animation frame
      await new Promise(resolve => requestAnimationFrame(resolve));
      
      // Step 5: Redraw all drawings from backend response
      const drawings = response.drawings || [];
      console.log('🎨 Redrawing', drawings.length, 'drawings after undo');
      
      if (drawings.length > 0) {
        await redrawCanvasFromData(drawings);
      } else {
        console.log('📝 No drawings to redraw (canvas is empty)');
      }
      
      // Step 6: Update local state
      setHistory(drawings);
      
      // Step 7: Sync backend state to update button states
      await syncWithBackend();
      
      console.log('✅ Undo operation complete. New drawings count:', drawings.length);
      return true;
      
    } catch (error) {
      console.error('❌ Undo error:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser.id, backendState.canUndo, isLoading, redrawCanvasFromData, syncWithBackend]);

  // ✅ CRITICAL FIX: Redo with proper canvas access
  const redo = useCallback(async () => {
    if (!meetingId || !currentUser.id || !backendState.canRedo || isLoading) {
      console.warn('⚠️ Cannot redo');
      return false;
    }
    
    setIsLoading(true);
    
    try {
      console.log('🔄 Redo - Current drawings:', history?.length);
      
      const response = await whiteboardAPI.redoAction(meetingId, currentUser.id);
      
      if (!response.success) {
        console.error('❌ Redo failed:', response.error);
        setIsLoading(false);
        return false;
      }
      
      const drawings = response.drawings || [];
      console.log('✅ After redo - drawings:', drawings.length);
      
      // ✅ CRITICAL FIX: Get actual canvas via getCanvas()
      const canvas = canvasRef.current?.getCanvas?.();
      
      if (canvas) {
        const context = canvas.getContext('2d');
        
        context.clearRect(0, 0, canvas.width, canvas.height);
        context.fillStyle = '#ffffff';
        context.fillRect(0, 0, canvas.width, canvas.height);
        
        console.log('🎨 Canvas cleared, redrawing', drawings.length, 'drawings');
        
        await redrawCanvasFromData(drawings);
      } else {
        console.error('❌ Cannot access canvas for redraw');
      }
      
      setHistory(drawings);
      
      try {
        await syncWithBackend();
      } catch (syncError) {
        console.error('Sync error after redo:', syncError);
      }
      
      setIsLoading(false);
      return true;
      
    } catch (error) {
      console.error('❌ Redo error:', error);
      setIsLoading(false);
      return false;
    }
  }, [meetingId, currentUser.id, backendState.canRedo, isLoading, redrawCanvasFromData, syncWithBackend, history]);

  const goBack = useCallback(async () => {
    if (!meetingId || !currentUser.id || !backendState.canGoBack) {
      console.warn('⚠️ Cannot go back');
      return false;
    }

    setIsLoading(true);
    
    try {
      console.log('🔄 Navigating backward...');
      
      const historyResponse = await whiteboardAPI.getHistory(meetingId, currentUser.id);
      
      if (historyResponse.success && historyResponse.history.checkpoints?.length > 0) {
        const checkpoints = historyResponse.history.checkpoints;
        const currentIndex = backendState.currentCheckpoint;
        
        if (currentIndex > 0) {
          const previousCheckpoint = checkpoints[currentIndex - 1];
          
          const navResponse = await whiteboardAPI.navigateToState(
            meetingId, 
            currentUser.id, 
            previousCheckpoint.id
          );
          
          if (navResponse.success) {
            const drawings = navResponse.drawings || [];
            await redrawCanvasFromData(drawings);
            setHistory(drawings);
            await syncWithBackend();
            
            console.log('✅ Navigation backward completed');
            return true;
          }
        }
      }
      
      return false;
    } catch (error) {
      console.error('❌ Error during backward navigation:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser.id, backendState.canGoBack, backendState.currentCheckpoint, redrawCanvasFromData, syncWithBackend]);

  const goForward = useCallback(async () => {
    if (!meetingId || !currentUser.id || !backendState.canGoForward) {
      console.warn('⚠️ Cannot go forward');
      return false;
    }

    setIsLoading(true);
    
    try {
      console.log('🔄 Navigating forward...');
      
      const historyResponse = await whiteboardAPI.getHistory(meetingId, currentUser.id);
      
      if (historyResponse.success && historyResponse.history.checkpoints?.length > 0) {
        const checkpoints = historyResponse.history.checkpoints;
        const currentIndex = backendState.currentCheckpoint;
        
        if (currentIndex < checkpoints.length - 1) {
          const nextCheckpoint = checkpoints[currentIndex + 1];
          
          const navResponse = await whiteboardAPI.navigateToState(
            meetingId, 
            currentUser.id, 
            nextCheckpoint.id
          );
          
          if (navResponse.success) {
            const drawings = navResponse.drawings || [];
            await redrawCanvasFromData(drawings);
            setHistory(drawings);
            await syncWithBackend();
            
            console.log('✅ Navigation forward completed');
            return true;
          }
        }
      }
      
      return false;
    } catch (error) {
      console.error('❌ Error during forward navigation:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser.id, backendState.canGoForward, backendState.currentCheckpoint, redrawCanvasFromData, syncWithBackend]);

  const createNavigationCheckpoint = useCallback(async (description = '') => {
    if (!meetingId || !currentUser.id) return null;
    
    try {
      console.log('📌 Creating navigation checkpoint:', { meetingId, userId: currentUser.id, description });
      const response = await whiteboardAPI.createCheckpoint(meetingId, currentUser.id, description);
      
      if (response.success) {
        await syncWithBackend();
        console.log('✅ Navigation checkpoint created');
        return response.checkpoint;
      }
    } catch (error) {
      console.error('❌ Error creating navigation checkpoint:', error);
    }
    
    return null;
  }, [meetingId, currentUser.id, syncWithBackend]);

  const getNavigationSummary = useCallback(async () => {
    if (!meetingId || !currentUser.id) return null;
    
    try {
      const response = await whiteboardAPI.getHistory(meetingId, currentUser.id);
      
      if (response.success) {
        return {
          total: response.history.checkpoints?.length || 0,
          current: backendState.currentCheckpoint + 1,
          canGoBack: backendState.canGoBack,
          canGoForward: backendState.canGoForward,
          hasHistory: response.history.checkpoints?.length > 1,
          checkpoints: response.history.checkpoints || []
        };
      }
    } catch (error) {
      console.error('❌ Error getting navigation summary:', error);
    }
    
    return null;
  }, [meetingId, currentUser.id, backendState]);

  const saveCanvasState = useCallback(async (isNavigationPoint = false) => {
    if (!meetingId || !currentUser.id) {
      console.warn('⚠️ Cannot save canvas state: missing meetingId or userId');
      return;
    }
    
    try {
      console.log('💾 Saving canvas state, isNavigationPoint:', isNavigationPoint);
      
      if (isNavigationPoint) {
        await createNavigationCheckpoint(`Navigation point at ${new Date().toLocaleTimeString()}`);
      }
      
      // ✅ FIXED: Get canvas via getCanvas()
      const canvas = canvasRef.current?.getCanvas?.();
      if (canvas) {
        const imageData = canvas.toDataURL();
        const newHistory = [...history, imageData];
        setHistory(newHistory);
        setHistoryIndex(newHistory.length - 1);
        console.log('✅ Canvas state saved locally');
      }
      
      await syncWithBackend();
      
    } catch (error) {
      console.error('❌ Error saving canvas state:', error);
    }
  }, [meetingId, currentUser.id, history, createNavigationCheckpoint, syncWithBackend]);

  const clear = useCallback(async () => {
    if (!meetingId || !currentUser.id) {
      console.warn('⚠️ Cannot clear: missing meetingId or userId');
      return;
    }
    
    try {
      setIsLoading(true);
      console.log('🗑️ Clearing whiteboard...');
      
      await createNavigationCheckpoint('Before Clear');
      
      const response = await whiteboardAPI.clearWhiteboard(meetingId, currentUser.id);
      
      if (response.success) {
        // ✅ FIXED: Get canvas via getCanvas()
        const canvas = canvasRef.current?.getCanvas?.();
        if (canvas) {
          const ctx = canvas.getContext('2d');
          ctx.clearRect(0, 0, canvas.width, canvas.height);
          ctx.fillStyle = '#ffffff';
          ctx.fillRect(0, 0, canvas.width, canvas.height);
          console.log('✅ Local canvas cleared');
        }
        
        setHistory([]);
        setHistoryIndex(-1);
        setDrawingCommands([]);
        
        await syncWithBackend();
        
        console.log('✅ Whiteboard cleared successfully');
      }
    } catch (error) {
      console.error('❌ Error clearing whiteboard:', error);
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser.id, createNavigationCheckpoint, syncWithBackend]);

  const broadcastDrawing = useCallback(async (drawingData) => {
    if (!meetingId || !currentUser.id) {
      console.warn('⚠️ Cannot broadcast drawing: missing meetingId or userId');
      return;
    }
    
    try {
      console.log('📡 Broadcasting drawing:', {
        tool: drawingData.tool,
        type: drawingData.type,
        pointsCount: drawingData.points?.length || 0,
        hasDrawingData: !!drawingData.drawing_data
      });
      
      // ✅ CRITICAL: Ensure complete data structure
      const enrichedData = {
        ...drawingData,
        meeting_id: meetingId,
        user_id: currentUser.id,
      };
      
      // ✅ Validate freehand drawings have proper structure
      const isFreehand = ['pen', 'pencil', 'brush', 'marker', 'highlighter', 'eraser'].includes(drawingData.tool);
      if (isFreehand) {
        if (!enrichedData.points || enrichedData.points.length === 0) {
          console.warn('⚠️ Freehand drawing has no points, skipping broadcast');
          return;
        }
        
        // Ensure drawing_data structure
        if (!enrichedData.drawing_data || !enrichedData.drawing_data.points) {
          enrichedData.drawing_data = {
            type: 'path',
            points: enrichedData.points
          };
        }
        
        console.log('✅ Broadcasting freehand path with', enrichedData.points.length, 'points');
      }
      
      const response = await whiteboardAPI.addDrawing(meetingId, enrichedData, currentUser.id);
      
      if (response.success) {
        console.log('✅ Drawing saved to backend, ID:', enrichedData.drawing_id);
        
        setDrawingCommands(prev => [...prev, {
          ...enrichedData,
          timestamp: Date.now()
        }]);
        
        const commandCount = drawingCommands.length + 1;
        if (commandCount % 15 === 0) {
          setTimeout(() => saveCanvasState(true), 100);
        }
        
        if (commandCount % 5 === 0) {
          setTimeout(syncWithBackend, 200);
        }
        
        console.log('✅ Drawing broadcasted successfully');
      } else {
        console.error('❌ Backend rejected drawing:', response.error);
      }
    } catch (error) {
      console.error('❌ Failed to broadcast drawing:', error);
    }
  }, [meetingId, currentUser.id, drawingCommands.length, saveCanvasState, syncWithBackend]);

  const exportWhiteboard = useCallback((format = 'png', name = 'whiteboard') => {
    try {
      console.log('📤 Exporting whiteboard:', { format, name });
      
      // ✅ FIXED: Get canvas via getCanvas()
      const canvas = canvasRef.current?.getCanvas?.();
      if (!canvas) {
        throw new Error('Canvas is not available for export');
      }
      
      const link = document.createElement('a');
      link.download = `${name}.${format}`;
      link.href = canvas.toDataURL(`image/${format}`);
      link.click();
      
      console.log('✅ Whiteboard exported successfully');
    } catch (error) {
      console.error('❌ Failed to export whiteboard:', error);
      throw error;
    }
  }, []);

  const saveWhiteboard = useCallback(async () => {
    if (!meetingId || !currentUser.id) {
      throw new Error('Cannot save: missing meeting ID or user ID');
    }
    
    try {
      console.log('💾 Saving whiteboard...');
      
      const response = await whiteboardAPI.createCheckpoint(
        meetingId, 
        currentUser.id, 
        `Saved at ${new Date().toLocaleString()}`
      );
      
      if (response.success) {
        await syncWithBackend();
        console.log('✅ Whiteboard saved successfully');
        return response;
      } else {
        throw new Error(response.error || 'Failed to save whiteboard');
      }
    } catch (error) {
      console.error('❌ Failed to save whiteboard:', error);
      throw error;
    }
  }, [meetingId, currentUser.id, syncWithBackend]);

  const loadWhiteboard = useCallback(async () => {
    if (!meetingId) {
      console.warn('⚠️ Cannot load whiteboard: no meetingId');
      return;
    }
    
    try {
      setIsLoading(true);
      console.log('📥 Loading whiteboard...');
      
      const response = await whiteboardAPI.getWhiteboardState(meetingId);
      
      if (response.success && response.whiteboard) {
        const drawings = response.whiteboard.drawings || [];
        console.log('📥 Loading', drawings.length, 'drawings from backend');
        
        const isReady = await ensureCanvasReady();
        
        if (isReady) {
          await redrawCanvasFromData(drawings);
        } else {
          console.warn('⚠️ Canvas not ready, storing drawings for later');
        }
        
        setHistory(drawings);
        await syncWithBackend();
        
        console.log('✅ Whiteboard loaded successfully');
      }
    } catch (error) {
      console.error('❌ Failed to load whiteboard:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, redrawCanvasFromData, syncWithBackend]);

  const importWhiteboard = useCallback((file) => {
    return new Promise((resolve, reject) => {
      try {
        // ✅ FIXED: Get canvas via getCanvas()
        const canvas = canvasRef.current?.getCanvas?.();
        if (!file || !canvas) {
          reject(new Error('Invalid file or canvas not ready'));
          return;
        }
        
        const ctx = canvas.getContext('2d');
        
        createNavigationCheckpoint('Before Import').then(() => {
          const reader = new FileReader();
          reader.onload = (e) => {
            const img = new Image();
            img.onload = () => {
              try {
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(img, 0, 0);
                saveCanvasState(true).then(resolve);
              } catch (error) {
                reject(new Error('Failed to draw imported image: ' + error.message));
              }
            };
            img.onerror = () => reject(new Error('Failed to load imported image'));
            img.src = e.target.result;
          };
          reader.onerror = () => reject(new Error('Failed to read file'));
          reader.readAsDataURL(file);
        });
      } catch (error) {
        reject(new Error('Import failed: ' + error.message));
      }
    });
  }, [createNavigationCheckpoint, saveCanvasState]);

  const onRemoteDrawing = useCallback((callback) => {
    console.log('🔗 Setting up remote drawing listener');
  }, []);

  // ✅ Only one useEffect for initialization
  useEffect(() => {
    if (meetingId && currentUser.id) {
      console.log('🚀 Initializing whiteboard for meeting:', meetingId);
      syncWithBackend();
      
      const syncInterval = setInterval(syncWithBackend, 10000);
      
      return () => {
        console.log('🔄 Cleaning up whiteboard sync interval');
        clearInterval(syncInterval);
      };
    }
  }, [meetingId, currentUser.id, syncWithBackend]);

  console.log('useWhiteboard state:', {
    meetingId,
    userId: currentUser.id,
    canUndo: backendState.canUndo,
    canRedo: backendState.canRedo,
    undoCount: backendState.undoCount,
    redoCount: backendState.redoCount,
    isLoading
  });

  return {
    tool,
    setTool,
    color,
    setColor,
    brushSize,
    setBrushSize,
    isDrawing,
    setIsDrawing,
    history,
    participants,
    highContrast,
    setHighContrast,
    isLoading,
    setCanvasRef,
    canUndo: backendState.canUndo,
    canRedo: backendState.canRedo,
    undo,
    redo,
    clear,
    saveCanvasState,
    canGoBack: backendState.canGoBack,
    canGoForward: backendState.canGoForward,
    hasNavigationHistory: backendState.checkpoints.length > 1,
    goBack,
    goForward,
    createNavigationCheckpoint,
    getNavigationSummary,
    navigationHistory: backendState.checkpoints,
    navigationIndex: backendState.currentCheckpoint,
    isNavigating: isLoading,
    saveWhiteboard,
    loadWhiteboard,
    exportWhiteboard,
    importWhiteboard,
    broadcastDrawing,   
    onRemoteDrawing,
    drawingCommands,
    snapshots,
    syncWithBackend,
    backendState,
    textFormat,
    setTextFormat,
    redrawCanvasFromData
  };
};  