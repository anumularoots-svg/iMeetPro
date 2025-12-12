import React, { useState, useRef, useEffect, useCallback, memo } from "react";
import {
  Box,
  Paper,
  IconButton,
  Typography, 
  Tooltip,
  Fade,
  Alert,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Switch,
  FormControlLabel,
  Snackbar,
  LinearProgress,
  Badge,
  Chip,
  Slider,
  ButtonGroup,
  Divider,
  Stack,
} from "@mui/material";
import {
  Close,
  Fullscreen,
  FullscreenExit,
  People,
  Save,
  Warning,
  Dashboard as WhiteboardIcon,
  Download,
  CheckCircle,
  Error as ErrorIcon,
  CloudSync,
  Refresh,
  Wifi,
  WifiOff,
  Storage,
  Update,
  Timeline,
  NavigateBefore,
  NavigateNext,
  Undo,
  Redo,
  Clear,
  
  // Professional Tool Icons - Updated to match reference
  NearMe,           // Selection/Cursor
  Create,           // Pen
  Edit,             // Pencil  
  Brush,            // Brush
  BackspaceOutlined, // Eraser
  CropSquare,       // Rectangle
  RadioButtonUnchecked, // Circle
  ChangeHistory,    // Triangle
  Remove,           // Line
  ArrowForward,     // Arrow
  TextFields,       // Text
  
   PaletteOutlined,
  LineWeight,
  Settings,
  Upload,
  Image,
  GetApp,
  Menu,
  GridOn,
  ZoomIn,
  ZoomOut,
  CenterFocusStrong,
} from "@mui/icons-material";
import WhiteboardCanvas from './WhiteboardCanvas';
import { useWhiteboard } from '../../hooks/useWhiteboard';

// Enhanced professional color palette with 120+ colors
const COLOR_PALETTE = [
  // Row 1 - Basic colors
  '#000000', '#FFFFFF', '#FF0000', '#00FF00', '#0000FF', '#FFFF00', '#FF00FF', '#00FFFF', '#FFA500', '#800080',
  
  // Row 2 - Reds
  '#FF6B6B', '#FF5252', '#F44336', '#E53935', '#D32F2F', '#C62828', '#B71C1C', '#FF1744', '#FF8A80', '#FFCDD2',
  
  // Row 3 - Pinks
  '#E91E63', '#AD1457', '#880E4F', '#F06292', '#EC407A', '#FF4081', '#FF80AB', '#FFB3BA', '#FFC0CB', '#FFE4E1',
  
  // Row 4 - Purples
  '#9C27B0', '#7B1FA2', '#4A148C', '#8E24AA', '#AB47BC', '#CE93D8', '#E1BEE7', '#DDA0DD', '#DA70D6', '#EE82EE',
  
  // Row 5 - Blues
  '#2196F3', '#1976D2', '#0D47A1', '#1E88E5', '#42A5F5', '#64B5F6', '#90CAF9', '#BBDEFB', '#87CEEB', '#87CEFA',
  
  // Row 6 - Cyans
  '#00BCD4', '#0097A7', '#006064', '#00ACC1', '#26C6DA', '#4DD0E1', '#80DEEA', '#B2EBF2', '#E0F2F1', '#AFEEEE',
  
  // Row 7 - Teals
  '#009688', '#00695C', '#004D40', '#00796B', '#26A69A', '#4DB6AC', '#80CBC4', '#B2DFDB', '#E0F2F1', '#20B2AA',
  
  // Row 8 - Greens
  '#4CAF50', '#388E3C', '#1B5E20', '#43A047', '#66BB6A', '#81C784', '#A5D6A7', '#C8E6C9', '#E8F5E8', '#90EE90',
  
  // Row 9 - Light Greens
  '#8BC34A', '#689F38', '#33691E', '#7CB342', '#9CCC65', '#AED581', '#C5E1A5', '#DCEDC8', '#F1F8E9', '#ADFF2F',
  
  // Row 10 - Yellows
  '#FFEB3B', '#F57F17', '#FF8F00', '#FBC02D', '#F9A825', '#F57C00', '#FFD54F', '#FFE082', '#FFF9C4', '#FFFFE0',
  
  // Row 11 - Oranges
  '#FF9800', '#E65100', '#BF360C', '#F57C00', '#FB8C00', '#FF9800', '#FFB74D', '#FFCC02', '#FFE0B2', '#FFEAA7',
  
  // Row 12 - Deep Oranges/Browns
  '#FF5722', '#BF360C', '#3E2723', '#D84315', '#F4511E', '#FF7043', '#FF8A65', '#FFAB91', '#A0522D', '#D2691E',
  
  // Row 13 - Browns
  '#795548', '#3E2723', '#1A0E0A', '#4E342E', '#5D4037', '#6D4C41', '#8D6E63', '#A1887F', '#BCAAA4', '#D7CCC8',
  
  // Row 14 - Grays
  '#607D8B', '#263238', '#37474F', '#455A64', '#546E7A', '#78909C', '#90A4AE', '#B0BEC5', '#CFD8DC', '#ECEFF1',
];

// Enhanced shapes configuration with more options
const TOOL_CONFIG = {
  selection: [
    { id: 'select', icon: NearMe, label: 'Selection', shortcut: 'V', description: 'Select and move objects' },
  ],
  drawing: [
    { id: 'pen', icon: Create, label: 'Pen', shortcut: 'P', description: 'Draw with pen' },
    { id: 'pencil', icon: Edit, label: 'Pencil', shortcut: 'N', description: 'Draw with pencil' },
    { id: 'brush', icon: Brush, label: 'Brush', shortcut: 'B', description: 'Paint with brush' },
    { id: 'eraser', icon: BackspaceOutlined, label: 'Eraser', shortcut: 'E', description: 'Erase drawings' },
  ],
  shapes: [
     { id: 'rectangle', icon: CropSquare, label: 'Rectangle', shortcut: 'R', description: 'Draw rectangle' },
  { id: 'square', icon: CropSquare, label: 'Square', shortcut: 'Shift+R', description: 'Draw perfect square' },
  { id: 'circle', icon: RadioButtonUnchecked, label: 'Circle', shortcut: 'O', description: 'Draw circle' },
  { id: 'ellipse', icon: RadioButtonUnchecked, label: 'Ellipse', shortcut: 'Shift+O', description: 'Draw ellipse' },
    { id: 'triangle', icon: ChangeHistory, label: 'Triangle', shortcut: 'T', description: 'Draw triangle' },
    { id: 'line', icon: Remove, label: 'Line', shortcut: 'L', description: 'Draw straight line' },
    { id: 'arrow', icon: ArrowForward, label: 'Arrow', shortcut: 'A', description: 'Draw arrow' },
  ],
  advancedShapes: [
    { id: 'diamond', icon: ChangeHistory, label: 'Diamond', shortcut: '', description: 'Draw diamond shape' },
    { id: 'pentagon', icon: ChangeHistory, label: 'Pentagon', shortcut: '', description: 'Draw pentagon' },
    { id: 'hexagon', icon: ChangeHistory, label: 'Hexagon', shortcut: '', description: 'Draw hexagon' },
    { id: 'star', icon: ChangeHistory, label: 'Star', shortcut: '', description: 'Draw star' },
    { id: 'heart', icon: ChangeHistory, label: 'Heart', shortcut: '', description: 'Draw heart' },
    { id: 'cloud', icon: ChangeHistory, label: 'Cloud', shortcut: '', description: 'Draw cloud' },
  ],
  text: [
    { id: 'text', icon: TextFields, label: 'Text', shortcut: 'Shift+T', description: 'Add text' },
  ]
};
const Whiteboard = ({ 
  meetingId,
  currentUser,
  participants = [],
  isHost = false,
  socket,
  onClose,
  isOpen = true,
  className,
  sx,
  onError,
  onSuccess,
}) => {
  const canvasRef = useRef(null);
  const containerRef = useRef(null);

  // UI State
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [showGrid, setShowGrid] = useState(false);
  const [zoomLevel, setZoomLevel] = useState(100);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showSettings, setShowSettings] = useState(false);
  
  // Tool panel states
  const [showColorPicker, setShowColorPicker] = useState(false);
  const [showToolSettings, setShowToolSettings] = useState(false);
  const [opacity, setOpacity] = useState(100);
  
  // Backend cache status states
  const [syncNotification, setSyncNotification] = useState({ open: false, type: 'info', message: '' });
  const [isOnline, setIsOnline] = useState(navigator.onLine);
  const [syncProgress, setSyncProgress] = useState(0);
  // Add this state initialization
const [textFormat, setTextFormat] = useState({
  fontSize: 20,
  bold: false,
  italic: false,
  underline: false,
  strikethrough: false,
  fontFamily: 'Arial'
});
  // Use the enhanced whiteboard hook
  const whiteboardHookResult = useWhiteboard(meetingId);
  
  const {
    tool = 'pen',
    setTool = () => {},
    color = '#000000',
    setColor = () => {},
    brushSize = 3,
    setBrushSize = () => {},
    isDrawing = false,
    history = [],
    participants: whiteboardParticipants = [],
    canUndo = false,
    canRedo = false,
    undo = () => {},
    redo = () => {},
    clear = () => {},
    saveWhiteboard = async () => {},
    loadWhiteboard = async () => {},
    exportWhiteboard = async () => {},
    importWhiteboard = async () => {},
    broadcastDrawing = () => {},
    onRemoteDrawing = () => {},
    saveCanvasState = () => {},
    setCanvasRef = () => {},
    canGoBack = false,
    canGoForward = false,
    goBack = () => {},
    goForward = () => {},
    syncWithBackend = () => {},
    backendState = {
      undoCount: 0,
      redoCount: 0,
      checkpoints: [],
      currentCheckpoint: -1
    },
    isLoading: hookIsLoading = false,
  } = whiteboardHookResult || {};

  // In Whiteboard.jsx - around line 200-250 where you have action handlers

const handleClear = useCallback(async () => {
  const confirmed = window.confirm('Are you sure you want to clear the whiteboard? This action cannot be undone.');
  if (!confirmed) return;
  
  try {
    setIsLoading(true);
    console.log('🗑 Clearing whiteboard...');
    
    // Step 1: Clear via hook (which calls backend)
    if (clear && typeof clear === 'function') {
      console.log('Calling clear function from hook...');
      await clear();
    }
    
    // Step 2: Force canvas clear locally via imperative handle
    if (canvasRef.current && canvasRef.current.clear) {
      canvasRef.current.clear();
    }
    
    // Step 3: Sync with backend
    if (syncWithBackend && typeof syncWithBackend === 'function') {
      console.log('Syncing with backend after clear...');
      await syncWithBackend();
    }
    
    setSyncNotification({ 
      open: true, 
      type: 'success', 
      message: 'Whiteboard cleared successfully' 
    });
    
    onSuccess?.('Whiteboard cleared successfully');
    
  } catch (err) {
    console.error('Clear operation failed:', err);
    setSyncNotification({ 
      open: true, 
      type: 'error', 
      message: `Failed to clear whiteboard: ${err.message}`
    });
    onError?.(err);
  } finally {
    setIsLoading(false);
  }
}, [clear, syncWithBackend, canvasRef, onSuccess, onError]);
   // Action handlers
const handleUndo = useCallback(async () => {
  if (!canUndo || isLoading) return;
  try {
    setIsLoading(true);
    await undo();
    setSyncNotification({ open: true, type: 'info', message: 'Action undone' });
  } catch (error) {
    console.error('Undo failed:', error);
    setSyncNotification({ open: true, type: 'error', message: 'Undo failed' });
  } finally {
    setIsLoading(false);
  }
}, [undo, canUndo, isLoading]);
const handleRedo = useCallback(async () => {
  if (!canRedo || isLoading) return;
  try {
    setIsLoading(true);
    await redo();
    setSyncNotification({ open: true, type: 'info', message: 'Action redone' });
  } catch (error) {
    console.error('Redo failed:', error);
    setSyncNotification({ open: true, type: 'error', message: 'Redo failed' });
  } finally {
    setIsLoading(false);
  }
}, [redo, canRedo, isLoading]);
  const handleSave = useCallback(async () => {
    if (!saveWhiteboard) return;
    try {
      setIsLoading(true);
      await saveWhiteboard();
      setSyncNotification({ open: true, type: 'success', message: 'Whiteboard saved' });
      onSuccess?.('Whiteboard saved successfully');
    } catch (err) {
      setError('Failed to save whiteboard');
      onError?.(err);
    } finally {
      setIsLoading(false);
    }
  }, [saveWhiteboard, onSuccess, onError]);

 
  const handleExportPNG = useCallback(() => {
    try {
      if (exportWhiteboard) {
        exportWhiteboard('png');
        onSuccess?.('Whiteboard exported as PNG');
      }
    } catch (err) {
      setError('Failed to export as PNG');
      onError?.(err);
    }
  }, [exportWhiteboard, onSuccess, onError]);

  const handleFullscreen = useCallback(() => {
    if (!document.fullscreenElement) {
      containerRef.current?.requestFullscreen?.();
    } else {
      document.exitFullscreen?.();
    }
  }, []);


  // Enhanced canvas state saving with validation
  const saveCanvasStateWithValidation = useCallback(async () => {
    if (!canvasRef.current) {
      console.warn('Cannot save canvas state: no canvas reference');
      return;
    }
    
    const canvas = canvasRef.current;
    
    // Validate canvas before attempting to save
    if (typeof canvas.toDataURL !== 'function') {
      console.error('Canvas does not have toDataURL method:', canvas);
      return;
    }
    
    try {
      if (saveCanvasState && typeof saveCanvasState === 'function') {
        await saveCanvasState();
        console.log('Canvas state saved successfully');
      }
    } catch (error) {
      console.error('Failed to save canvas state:', error);
    }
  }, [saveCanvasState]);


    const handleDrawing = useCallback((drawingData) => {
    if (broadcastDrawing && typeof broadcastDrawing === 'function') {
      broadcastDrawing(drawingData);
    }
    
    // Only save canvas state if canvas is properly initialized
    if (saveCanvasState && canvasRef.current && typeof canvasRef.current.toDataURL === 'function') {
      try {
        saveCanvasState();
      } catch (error) {
        console.warn('Failed to save canvas state during drawing:', error);
      }
    }
  }, [broadcastDrawing, saveCanvasState]);

  const handleZoomIn = useCallback(() => {
    const newZoom = Math.min(zoomLevel + 25, 500);
    setZoomLevel(newZoom);
    
    // Apply zoom to canvas if available
    if (canvasRef.current && canvasRef.current.style) {
      canvasRef.current.style.transform = `scale(${newZoom / 100})`;
      canvasRef.current.style.transformOrigin = 'top left';
    }
    
    console.log('Zoomed in to:', newZoom + '%');
  }, [zoomLevel]);

  const handleZoomOut = useCallback(() => {
    const newZoom = Math.max(zoomLevel - 25, 25);
    setZoomLevel(newZoom);
    
    // Apply zoom to canvas if available
    if (canvasRef.current && canvasRef.current.style) {
      canvasRef.current.style.transform = `scale(${newZoom / 100})`;
      canvasRef.current.style.transformOrigin = 'top left';
    }
    
    console.log('Zoomed out to:', newZoom + '%');
  }, [zoomLevel]);

  const handleZoomReset = useCallback(() => {
    setZoomLevel(100);
    
    // Reset canvas zoom
    if (canvasRef.current && canvasRef.current.style) {
      canvasRef.current.style.transform = 'scale(1)';
      canvasRef.current.style.transformOrigin = 'top left';
    }
    
    console.log('Zoom reset to: 100%');
  }, []);

  // Enhanced zoom to fit functionality
  const handleZoomToFit = useCallback(() => {
    if (!canvasRef.current?.parentElement) return;
    
    const canvas = canvasRef.current;
    const container = canvas.parentElement;
    
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;
    const canvasWidth = canvas.width;
    const canvasHeight = canvas.height;
    
    // Calculate scale to fit
    const scaleX = containerWidth / canvasWidth;
    const scaleY = containerHeight / canvasHeight;
    const scale = Math.min(scaleX, scaleY, 1); // Don't zoom in beyond 100%
    
    const newZoom = Math.round(scale * 100);
    setZoomLevel(newZoom);
    
    if (canvas.style) {
      canvas.style.transform = `scale(${scale})`;
      canvas.style.transformOrigin = 'top left';
    }
    
    console.log('Zoom to fit:', newZoom + '%');
  }, []);


  // Get current tool info
  const getCurrentToolInfo = () => {
    const allTools = [...TOOL_CONFIG.selection, ...TOOL_CONFIG.drawing, ...TOOL_CONFIG.shapes, ...TOOL_CONFIG.text];
    return allTools.find(t => t.id === tool) || { label: 'Unknown', description: '' };
  };

  // Helper function to test functions
  const testButtonFunctions = useCallback(() => {
    console.log('=== Button Function Test ===');
    console.log('Testing undo function...');
    if (undo && typeof undo === 'function') {
      console.log('✅ Undo function is available and callable');
    } else {
      console.log('❌ Undo function is not available');
    }
    
    console.log('Testing redo function...');
    if (redo && typeof redo === 'function') {
      console.log('✅ Redo function is available and callable');
    } else {
      console.log('❌ Redo function is not available');
    }
    
    console.log('Testing clear function...');
    if (clear && typeof clear === 'function') {
      console.log('✅ Clear function is available and callable');
    } else {
      console.log('❌ Clear function is not available');
    }
    console.log('===========================');
  }, [undo, redo, clear]);
useEffect(() => {
  console.log('Button states:', {
    canUndo,
    canRedo,
    undoCount: backendState.undoCount,
    redoCount: backendState.redoCount,
    isLoading,
    hookIsLoading
  });
}, [canUndo, canRedo, backendState, isLoading, hookIsLoading]);
  // Enhanced canvas reference setup with proper validation
// Enhanced canvas reference setup with proper validation
useEffect(() => {
  if (canvasRef.current && setCanvasRef) {
    const canvas = canvasRef.current;
    
    // ✅ FIXED: Better validation
    if (canvas && canvas.getCanvas && typeof canvas.getCanvas === 'function') {
      const actualCanvas = canvas.getCanvas();
      if (actualCanvas && typeof actualCanvas.getContext === 'function' && typeof actualCanvas.toDataURL === 'function') {
        setCanvasRef(canvasRef);
        console.log('✅ Canvas reference set successfully');
      }
    } else {
      console.warn('Canvas not fully initialized, waiting...');
      // Retry after a short delay
      const retryTimer = setTimeout(() => {
        if (canvas && canvas.getCanvas) {
          setCanvasRef(canvasRef);
        }
      }, 200);
      return () => clearTimeout(retryTimer);
    }
  }
}, [setCanvasRef]);
  // Monitor online/offline status
  useEffect(() => {
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  // Load whiteboard data when opened
  useEffect(() => {
    if (isOpen && meetingId && loadWhiteboard) {
      setIsLoading(true);
      loadWhiteboard()
        .then(() => setError(null))
        .catch((err) => {
          setError('Failed to load whiteboard data');
          onError?.(err);
        })
        .finally(() => setIsLoading(false));
    }
  }, [isOpen, meetingId, loadWhiteboard, onError]);

  // Enhanced keyboard shortcuts - FIXED
 useEffect(() => {
  const handleKeyboard = (event) => {
    if (!isOpen || 
        event.target.tagName === 'INPUT' || 
        event.target.tagName === 'TEXTAREA' ||
        event.target.isContentEditable) {
      return;
    }

    // Ctrl/Cmd combinations
    if (event.ctrlKey || event.metaKey) {
      switch (event.key.toLowerCase()) {
        case 'z':
          event.preventDefault();
          if (event.shiftKey) {
            handleRedo();
          } else {
            handleUndo();
          }
          break;
        case 'y':
          event.preventDefault();
          handleRedo();
          break;
        case 's':
          event.preventDefault();
          handleSave();
          break;
        default:
          break;
      }
      return;
    }

    // Tool shortcuts
    const key = event.key.toLowerCase();
    switch (key) {
      case 'delete':
      case 'backspace':
        if (event.shiftKey) {
          event.preventDefault();
          handleClear();
        }
        break;
      default:
        break;
    }
  };

  document.addEventListener('keydown', handleKeyboard);
  return () => document.removeEventListener('keydown', handleKeyboard);
}, [isOpen, handleUndo, handleRedo, handleSave, handleClear]);

  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };
    document.addEventListener('fullscreenchange', handleFullscreenChange);
    return () => document.removeEventListener('fullscreenchange', handleFullscreenChange);
  }, []);



  // Debug logging effect to help diagnose issues
  useEffect(() => {
    console.log('=== Whiteboard Button State Debug ===');
    console.log('canUndo:', canUndo, typeof canUndo);
    console.log('canRedo:', canRedo, typeof canRedo);
    console.log('undo function:', typeof undo);
    console.log('redo function:', typeof redo);
    console.log('clear function:', typeof clear);
    console.log('isLoading:', isLoading);
    console.log('hookIsLoading:', hookIsLoading);
    console.log('backendState:', backendState);
    console.log('meetingId:', meetingId);
    console.log('=====================================');
  }, [canUndo, canRedo, undo, redo, clear, isLoading, hookIsLoading, backendState, meetingId]);



  if (!isOpen) return null;

  return (
    <Fade in={isOpen} timeout={300}>
      <Paper
        ref={containerRef}
        className={className}
        elevation={0}
        sx={{
          height: isFullscreen ? '100vh' : '100vh',
          width: isFullscreen ? '100vw' : '100%',
          display: 'flex',
          position: isFullscreen ? 'fixed' : 'relative',
          top: isFullscreen ? 0 : 'auto',
          left: isFullscreen ? 0 : 'auto',
          right: isFullscreen ? 0 : 'auto',
          bottom: isFullscreen ? 0 : 'auto',
          backgroundColor: '#f5f6fa',
          overflow: 'hidden',
          zIndex: isFullscreen ? 50000 : 'auto',
          ...sx,
        }}
      >
        {/* Sync Progress Bar */}
        {syncProgress > 0 && (
          <LinearProgress 
            variant="determinate" 
            value={syncProgress}
            sx={{ 
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              zIndex: 1001,
            }}
          />
        )}

        {/* Professional Left Sidebar - Matching Reference */}
        <Paper
          elevation={0}
          sx={{
            width: 200,
            backgroundColor: '#ffffff',
            borderRight: '1px solid #e1e5e9',
            display: 'flex',
            flexDirection: 'column',
            zIndex: 10,
          }}
        >
          {/* Header with Logo */}
          <Box sx={{ 
            p: 2,
            borderBottom: '1px solid #e1e5e9',
            backgroundColor: '#ffffff'
          }}>
            <Stack direction="row" alignItems="center" spacing={1}>
              <Box sx={{
                width: 24,
                height: 24,
                backgroundColor: '#1976d2',
                borderRadius: '4px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}>
                <WhiteboardIcon sx={{ fontSize: '14px', color: 'white' }} />
              </Box>
              <Typography variant="body2" sx={{ 
                fontSize: '13px', 
                fontWeight: 600, 
                color: '#2c3e50'
              }}>
                Whiteboard Pro
              </Typography>
            </Stack>
          </Box>

          {/* Tool Sections */}
          <Box sx={{ flex: 1, overflow: 'auto', p: 1 }}>
            
            {/* Selection Section */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="caption" sx={{ 
                color: '#7f8c8d', 
                fontWeight: 600, 
                fontSize: '10px', 
                letterSpacing: '0.5px',
                textTransform: 'uppercase',
                px: 1,
                mb: 1,
                display: 'block'
              }}>
                Selection
              </Typography>
              <Stack spacing={0.5}>
                {TOOL_CONFIG.selection.map((toolItem) => {
                  const IconComponent = toolItem.icon;
                  const isActive = tool === toolItem.id;
                  return (
                    <Tooltip 
                      key={toolItem.id} 
                      title={`${toolItem.label} (${toolItem.shortcut})`} 
                      placement="right"
                    >
                      <IconButton
                        onClick={() => setTool(toolItem.id)}
                        sx={{
                          width: '100%',
                          height: 32,
                          borderRadius: '6px',
                          backgroundColor: isActive ? '#e3f2fd' : 'transparent',
                          justifyContent: 'flex-start',
                          paddingLeft: 1.5,
                          '&:hover': {
                            backgroundColor: isActive ? '#e3f2fd' : '#f8f9fa',
                          }
                        }}
                      >
                        <IconComponent 
                          sx={{ 
                            fontSize: '16px',
                            color: isActive ? '#1976d2' : '#5a6c7d',
                            mr: 1
                          }} 
                        />
                        <Typography variant="caption" sx={{
                          fontSize: '12px',
                          color: isActive ? '#1976d2' : '#5a6c7d',
                          fontWeight: isActive ? 600 : 400
                        }}>
                          {toolItem.label}
                        </Typography>
                      </IconButton>
                    </Tooltip>
                  );
                })}
              </Stack>
            </Box>

            {/* Drawing Section */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="caption" sx={{ 
                color: '#7f8c8d', 
                fontWeight: 600, 
                fontSize: '10px', 
                letterSpacing: '0.5px',
                textTransform: 'uppercase',
                px: 1,
                mb: 1,
                display: 'block'
              }}>
                Drawing
              </Typography>
              <Stack spacing={0.5}>
                {TOOL_CONFIG.drawing.map((toolItem) => {
                  const IconComponent = toolItem.icon;
                  const isActive = tool === toolItem.id;
                  return (
                    <Tooltip 
                      key={toolItem.id} 
                      title={`${toolItem.label} (${toolItem.shortcut})`} 
                      placement="right"
                    >
                      <IconButton
                        onClick={() => setTool(toolItem.id)}
                        sx={{
                          width: '100%',
                          height: 32,
                          borderRadius: '6px',
                          backgroundColor: isActive ? '#e3f2fd' : 'transparent',
                          justifyContent: 'flex-start',
                          paddingLeft: 1.5,
                          '&:hover': {
                            backgroundColor: isActive ? '#e3f2fd' : '#f8f9fa',
                          }
                        }}
                      >
                        <IconComponent 
                          sx={{ 
                            fontSize: '16px',
                            color: isActive ? '#1976d2' : '#5a6c7d',
                            mr: 1
                          }} 
                        />
                        <Typography variant="caption" sx={{
                          fontSize: '12px',
                          color: isActive ? '#1976d2' : '#5a6c7d',
                          fontWeight: isActive ? 600 : 400
                        }}>
                          {toolItem.label}
                        </Typography>
                      </IconButton>
                    </Tooltip>
                  );
                })}
              </Stack>
            </Box>

            {/* Shapes Section */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="caption" sx={{ 
                color: '#7f8c8d', 
                fontWeight: 600, 
                fontSize: '10px', 
                letterSpacing: '0.5px',
                textTransform: 'uppercase',
                px: 1,
                mb: 1,
                display: 'block'
              }}>
                Shapes
              </Typography>
              <Stack spacing={0.5}>
                {TOOL_CONFIG.shapes.map((toolItem) => {
                  const IconComponent = toolItem.icon;
                  const isActive = tool === toolItem.id;
                  return (
                    <Tooltip 
                      key={toolItem.id} 
                      title={`${toolItem.label} (${toolItem.shortcut})`} 
                      placement="right"
                    >
                      <IconButton
                        onClick={() => setTool(toolItem.id)}
                        sx={{
                          width: '100%',
                          height: 32,
                          borderRadius: '6px',
                          backgroundColor: isActive ? '#e3f2fd' : 'transparent',
                          justifyContent: 'flex-start',
                          paddingLeft: 1.5,
                          '&:hover': {
                            backgroundColor: isActive ? '#e3f2fd' : '#f8f9fa',
                          }
                        }}
                      >
                        <IconComponent 
                          sx={{ 
                            fontSize: '16px',
                            color: isActive ? '#1976d2' : '#5a6c7d',
                            mr: 1
                          }} 
                        />
                        <Typography variant="caption" sx={{
                          fontSize: '12px',
                          color: isActive ? '#1976d2' : '#5a6c7d',
                          fontWeight: isActive ? 600 : 400
                        }}>
                          {toolItem.label}
                        </Typography>
                      </IconButton>
                    </Tooltip>
                  );
                })}
              </Stack>
            </Box>

            {/* Text Section */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="caption" sx={{ 
                color: '#7f8c8d', 
                fontWeight: 600, 
                fontSize: '10px', 
                letterSpacing: '0.5px',
                textTransform: 'uppercase',
                px: 1,
                mb: 1,
                display: 'block'
              }}>
                Content
              </Typography>
              <Stack spacing={0.5}>
                {TOOL_CONFIG.text.map((toolItem) => {
                  const IconComponent = toolItem.icon;
                  const isActive = tool === toolItem.id;
                  return (
                    <Tooltip 
                      key={toolItem.id} 
                      title={`${toolItem.label} (${toolItem.shortcut})`} 
                      placement="right"
                    >
                      <IconButton
                        onClick={() => setTool(toolItem.id)}
                        sx={{
                          width: '100%',
                          height: 32,
                          borderRadius: '6px',
                          backgroundColor: isActive ? '#e3f2fd' : 'transparent',
                          justifyContent: 'flex-start',
                          paddingLeft: 1.5,
                          '&:hover': {
                            backgroundColor: isActive ? '#e3f2fd' : '#f8f9fa',
                          }
                        }}
                      >
                        <IconComponent 
                          sx={{ 
                            fontSize: '16px',
                            color: isActive ? '#1976d2' : '#5a6c7d',
                            mr: 1
                          }} 
                        />
                        <Typography variant="caption" sx={{
                          fontSize: '12px',
                          color: isActive ? '#1976d2' : '#5a6c7d',
                          fontWeight: isActive ? 600 : 400
                        }}>
                          {toolItem.label}
                        </Typography>
                      </IconButton>
                    </Tooltip>
                  );
                })}
              </Stack>
              {/* Text Formatting Section - Add this after the "Content" section */}
{tool === 'text' && (
  <Box sx={{ mb: 2 }}>
    <Typography variant="caption" sx={{ 
      color: '#7f8c8d', 
      fontWeight: 600, 
      fontSize: '10px', 
      letterSpacing: '0.5px',
      textTransform: 'uppercase',
      px: 1,
      mb: 1,
      display: 'block'
    }}>
      Text Formatting
    </Typography>
    
    <Stack spacing={1} sx={{ px: 1 }}>
      {/* Font Size */}
      <Box>
        <Typography variant="caption" sx={{ fontSize: '10px', color: '#7f8c8d', mb: 0.5, display: 'block' }}>
          Font Size: {textFormat.fontSize}px
        </Typography>
        <Slider
          value={textFormat.fontSize}
          onChange={(e, value) => setTextFormat(prev => ({ ...prev, fontSize: value }))}
          min={12}
          max={72}
          size="small"
          sx={{ 
            color: '#1976d2',
            height: 4,
            '& .MuiSlider-thumb': {
              width: 12,
              height: 12,
              backgroundColor: '#1976d2',
              border: '2px solid #ffffff',
            },
            '& .MuiSlider-track': {
              backgroundColor: '#1976d2',
              border: 'none',
              height: 4,
            },
            '& .MuiSlider-rail': {
              backgroundColor: '#e1e5e9',
              height: 4,
            }
          }}
        />
      </Box>

      {/* Font Style Buttons */}
      <Stack direction="row" spacing={0.5}>
        <Tooltip title="Bold">
          <IconButton
            size="small"
            onClick={() => setTextFormat(prev => ({ ...prev, bold: !prev.bold }))}
            sx={{
              width: 32,
              height: 32,
              backgroundColor: textFormat.bold ? '#e3f2fd' : 'transparent',
              color: textFormat.bold ? '#1976d2' : '#5a6c7d',
              border: `1px solid ${textFormat.bold ? '#1976d2' : '#e1e5e9'}`,
              '&:hover': {
                backgroundColor: '#e3f2fd',
              }
            }}
          >
            <strong style={{ fontSize: '14px', fontWeight: 'bold' }}>B</strong>
          </IconButton>
        </Tooltip>

        <Tooltip title="Italic">
          <IconButton
            size="small"
            onClick={() => setTextFormat(prev => ({ ...prev, italic: !prev.italic }))}
            sx={{
              width: 32,
              height: 32,
              backgroundColor: textFormat.italic ? '#e3f2fd' : 'transparent',
              color: textFormat.italic ? '#1976d2' : '#5a6c7d',
              border: `1px solid ${textFormat.italic ? '#1976d2' : '#e1e5e9'}`,
              '&:hover': {
                backgroundColor: '#e3f2fd',
              }
            }}
          >
            <em style={{ fontSize: '14px', fontStyle: 'italic' }}>I</em>
          </IconButton>
        </Tooltip>

        <Tooltip title="Underline">
          <IconButton
            size="small"
            onClick={() => setTextFormat(prev => ({ ...prev, underline: !prev.underline }))}
            sx={{
              width: 32,
              height: 32,
              backgroundColor: textFormat.underline ? '#e3f2fd' : 'transparent',
              color: textFormat.underline ? '#1976d2' : '#5a6c7d',
              border: `1px solid ${textFormat.underline ? '#1976d2' : '#e1e5e9'}`,
              '&:hover': {
                backgroundColor: '#e3f2fd',
              }
            }}
          >
            <span style={{ fontSize: '14px', textDecoration: 'underline' }}>U</span>
          </IconButton>
        </Tooltip>

        <Tooltip title="Strikethrough">
          <IconButton
            size="small"
            onClick={() => setTextFormat(prev => ({ ...prev, strikethrough: !prev.strikethrough }))}
            sx={{
              width: 32,
              height: 32,
              backgroundColor: textFormat.strikethrough ? '#e3f2fd' : 'transparent',
              color: textFormat.strikethrough ? '#1976d2' : '#5a6c7d',
              border: `1px solid ${textFormat.strikethrough ? '#1976d2' : '#e1e5e9'}`,
              '&:hover': {
                backgroundColor: '#e3f2fd',
              }
            }}
          >
            <span style={{ fontSize: '14px', textDecoration: 'line-through' }}>S</span>
          </IconButton>
        </Tooltip>
      </Stack>

      {/* Font Family */}
      <Box>
        <Typography variant="caption" sx={{ fontSize: '10px', color: '#7f8c8d', mb: 0.5, display: 'block' }}>
          Font Family
        </Typography>
        <select
          value={textFormat.fontFamily}
          onChange={(e) => setTextFormat(prev => ({ ...prev, fontFamily: e.target.value }))}
          style={{
            width: '100%',
            padding: '6px',
            fontSize: '12px',
            borderRadius: '4px',
            border: '1px solid #e1e5e9',
            backgroundColor: '#ffffff',
            color: '#2c3e50',
            cursor: 'pointer'
          }}
        >
          <option value="Arial">Arial</option>
          <option value="Times New Roman">Times New Roman</option>
          <option value="Courier New">Courier New</option>
          <option value="Georgia">Georgia</option>
          <option value="Verdana">Verdana</option>
          <option value="Comic Sans MS">Comic Sans MS</option>
        </select>
      </Box>
    </Stack>
  </Box>
)}
            </Box>

            {/* Clear All Button - ENHANCED */}
            ? 
<Button
  fullWidth
  variant="outlined"
  size="small"
  onClick={(e) => {
    e.preventDefault();
    e.stopPropagation();
    handleClear();
  }}
  disabled={isLoading || hookIsLoading}
  startIcon={
    (isLoading || hookIsLoading) ? 
    <CircularProgress size={12} sx={{ color: '#dc2626' }} /> : 
    <Clear sx={{ fontSize: '14px' }} />
  }
  sx={{
    mt: 2,
    mx: 1,
    color: '#e74c3c',
    borderColor: '#e74c3c',
    backgroundColor: '#ffffff',
    borderRadius: '6px',
    textTransform: 'none',
    fontSize: '11px',
    fontWeight: 600,
    height: 32,
    border: '2px solid #e74c3c',
    '&:hover': {
      backgroundColor: '#fdf2f2',
      borderColor: '#c0392b',
      transform: 'translateY(-1px)',
      boxShadow: '0 3px 6px rgba(231, 76, 60, 0.3)'
    },
    '&:disabled': {
      color: '#bdc3c7',
      borderColor: '#ecf0f1',
      backgroundColor: '#f8f9fa'
    }
  }}
>
  {isLoading || hookIsLoading ? 'Clearing...' : 'Clear All'}
</Button>
          </Box>

          {/* Tool Settings Panel */}
          <Box sx={{ 
            borderTop: '1px solid #e1e5e9', 
            backgroundColor: '#fafbfc',
            p: 2
          }}>
            {/* Current Color Display */}
            <Box sx={{ mb: 2 }}>
              <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                <Typography variant="caption" sx={{ 
                  color: '#7f8c8d', 
                  fontSize: '10px',
                  fontWeight: 600,
                  textTransform: 'uppercase'
                }}>
                  Color
                </Typography>
                <Typography variant="caption" sx={{ 
                  color: '#7f8c8d', 
                  fontSize: '9px',
                  fontFamily: 'monospace'
                }}>
                  {color.toUpperCase()}
                </Typography>
              </Stack>
              <Box
                sx={{
                  width: '100%',
                  height: 24,
                  backgroundColor: color,
                  border: '1px solid #e1e5e9',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  '&:hover': {
                    borderColor: '#bdc3c7',
                  }
                }}
                onClick={() => setShowColorPicker(!showColorPicker)}
              />
            </Box>

            {/* Tool Settings */}
            <Box>
              <Typography variant="caption" sx={{ 
                color: '#7f8c8d', 
                fontSize: '10px',
                fontWeight: 600,
                textTransform: 'uppercase',
                display: 'block',
                mb: 1
              }}>
                Tool Settings
              </Typography>
              
              {/* Pen Tool Settings */}
              <Box sx={{ mb: 1 }}>
                <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 0.5 }}>
                  <Typography variant="caption" sx={{ fontSize: '10px', color: '#7f8c8d' }}>
                    Pen, Opacity, 100%
                  </Typography>
                  <Typography variant="caption" sx={{ fontSize: '9px', color: '#7f8c8d' }}>
                    5px
                  </Typography>
                </Stack>
              </Box>

              {/* Brush Size Slider */}
              <Box sx={{ mb: 2 }}>
                <Typography variant="caption" sx={{ 
                  fontSize: '10px', 
                  color: '#7f8c8d',
                  display: 'block',
                  mb: 0.5
                }}>
                  Brush Size
                </Typography>
                <Slider
                  value={brushSize}
                  onChange={(e, value) => setBrushSize(value)}
                  min={1}
                  max={50}
                  size="small"
                  sx={{ 
                    color: '#1976d2',
                    height: 4,
                    '& .MuiSlider-thumb': {
                      width: 12,
                      height: 12,
                      backgroundColor: '#1976d2',
                      border: '2px solid #ffffff',
                      '&:hover': {
                        boxShadow: '0 0 0 4px rgba(25, 118, 210, 0.16)',
                      }
                    },
                    '& .MuiSlider-track': {
                      backgroundColor: '#1976d2',
                      border: 'none',
                      height: 4,
                    },
                    '& .MuiSlider-rail': {
                      backgroundColor: '#e1e5e9',
                      height: 4,
                    }
                  }}
                />
              </Box>

              {/* Opacity Slider */}
              <Box>
                <Typography variant="caption" sx={{ 
                  fontSize: '10px', 
                  color: '#7f8c8d',
                  display: 'block',
                  mb: 0.5
                }}>
                  Opacity
                </Typography>
                <Slider
                  value={opacity}
                  onChange={(e, value) => setOpacity(value)}
                  min={10}
                  max={100}
                  size="small"
                  sx={{ 
                    color: '#1976d2',
                    height: 4,
                    '& .MuiSlider-thumb': {
                      width: 12,
                      height: 12,
                      backgroundColor: '#1976d2',
                      border: '2px solid #ffffff',
                      '&:hover': {
                        boxShadow: '0 0 0 4px rgba(25, 118, 210, 0.16)',
                      }
                    },
                    '& .MuiSlider-track': {
                      backgroundColor: '#1976d2',
                      border: 'none',
                      height: 4,
                    },
                    '& .MuiSlider-rail': {
                      backgroundColor: '#e1e5e9',
                      height: 4,
                    }
                  }}
                />
              </Box>
            </Box>
          </Box>
        </Paper>

        {/* Main Content Area */}
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
          {/* Top Toolbar */}
          <Paper
            elevation={0}
            sx={{
              height: 48,
              backgroundColor: '#ffffff',
              borderBottom: '1px solid #e1e5e9',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              px: 2,
              zIndex: 10,
            }}
          >
            {/* Left side - Navigation and history */}
            <Stack direction="row" alignItems="center" spacing={0.5}>
              <Tooltip title="Menu">
                <IconButton 
                  size="small" 
                  onClick={() => setShowSettings(!showSettings)}
                  sx={{ color: '#7f8c8d' }}
                >
                  <Menu fontSize="small" />
                </IconButton>
              </Tooltip>
              
              <Divider orientation="vertical" flexItem sx={{ mx: 1, height: 20 }} />
              {/* Undo Button - Enhanced */}
<Tooltip title={`Undo${canUndo ? ` (${backendState.undoCount || 0} available)` : ' (none)'}`}>
  <span>
    <IconButton 
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        handleUndo();
      }}
      disabled={!canUndo || isLoading || hookIsLoading}
      size="small"
      sx={{ 
        color: (canUndo && !isLoading && !hookIsLoading) ? '#5a6c7d' : '#bdc3c7',
        border: `2px solid ${(canUndo && !isLoading && !hookIsLoading) ? '#5a6c7d' : '#bdc3c7'}`,
        backgroundColor: '#ffffff',
        '&:hover': {
          backgroundColor: (canUndo && !isLoading && !hookIsLoading) ? 'rgba(90, 108, 125, 0.1)' : 'transparent',
          transform: (canUndo && !isLoading && !hookIsLoading) ? 'translateY(-1px)' : 'none',
          boxShadow: (canUndo && !isLoading && !hookIsLoading) ? '0 2px 4px rgba(90, 108, 125, 0.3)' : 'none'
        },
        '&:disabled': {
          cursor: 'not-allowed'
        }
      }}
    >
      <Undo fontSize="small" />
    </IconButton>
  </span>
</Tooltip>

{/* Redo Button - Enhanced */}
<Tooltip title={`Redo${canRedo ? ` (${backendState.redoCount || 0} available)` : ' (none)'}`}>
  <span>
    <IconButton 
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        handleRedo();
      }}
      disabled={!canRedo || isLoading || hookIsLoading}
      size="small"
      sx={{ 
        color: (canRedo && !isLoading && !hookIsLoading) ? '#5a6c7d' : '#bdc3c7',
        border: `2px solid ${(canRedo && !isLoading && !hookIsLoading) ? '#5a6c7d' : '#bdc3c7'}`,
        backgroundColor: '#ffffff',
        '&:hover': {
          backgroundColor: (canRedo && !isLoading && !hookIsLoading) ? 'rgba(90, 108, 125, 0.1)' : 'transparent',
          transform: (canRedo && !isLoading && !hookIsLoading) ? 'translateY(-1px)' : 'none',
          boxShadow: (canRedo && !isLoading && !hookIsLoading) ? '0 2px 4px rgba(90, 108, 125, 0.3)' : 'none'
        },
        '&:disabled': {
          cursor: 'not-allowed'
        }
      }}
    >
      <Redo fontSize="small" />
    </IconButton>
  </span>
</Tooltip>
            </Stack>

            {/* Center - File info */}
            <Stack direction="row" alignItems="center" spacing={2}>
              <Typography variant="body2" sx={{ 
                fontSize: '13px', 
                fontWeight: 500, 
                color: '#2c3e50' 
              }}>
                Canvas
              </Typography>
              <Typography variant="caption" sx={{ 
                fontSize: '11px', 
                color: '#7f8c8d' 
              }}>
                100% saved
              </Typography>
            </Stack>

            {/* Right side - View controls */}
            <Stack direction="row" alignItems="center" spacing={0.5}>
              <Chip
                label="Synced"
                size="small"
                sx={{
                  height: 22,
                  fontSize: '10px',
                  backgroundColor: isOnline ? '#d5f4e6' : '#ffeaa7',
                  color: isOnline ? '#00b894' : '#fdcb6e',
                  border: 'none',
                }}
              />
              
              <Divider orientation="vertical" flexItem sx={{ mx: 1, height: 20 }} />
              
              {/* Zoom Controls */}
              <Stack direction="row" alignItems="center" spacing={0.5}>
                <Tooltip title="Zoom Out (25%)">
                  <span>
                    <IconButton 
                      size="small" 
                      onClick={handleZoomOut}
                      disabled={zoomLevel <= 25}
                      sx={{ 
                        color: zoomLevel > 25 ? '#7f8c8d' : '#bdc3c7',
                        '&:hover': {
                          backgroundColor: zoomLevel > 25 ? '#f8fafc' : 'transparent'
                        }
                      }}
                    >
                      <ZoomOut fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>
                
                <Typography 
                  variant="caption" 
                  sx={{ 
                    fontSize: '11px', 
                    color: '#7f8c8d',
                    minWidth: '45px',
                    textAlign: 'center',
                    cursor: 'pointer',
                    padding: '4px 8px',
                    borderRadius: '4px',
                    '&:hover': {
                      backgroundColor: '#f8fafc'
                    }
                  }}
                  onClick={handleZoomReset}
                  title="Click to reset zoom to 100%"
                >
                  {zoomLevel}%
                </Typography>
                
                <Tooltip title="Zoom In (25%)">
                  <span>
                    <IconButton 
                      size="small" 
                      onClick={handleZoomIn}
                      disabled={zoomLevel >= 500}
                      sx={{ 
                        color: zoomLevel < 500 ? '#7f8c8d' : '#bdc3c7',
                        '&:hover': {
                          backgroundColor: zoomLevel < 500 ? '#f8fafc' : 'transparent'
                        }
                      }}
                    >
                      <ZoomIn fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>
                
                <Tooltip title="Fit to Screen">
                  <IconButton 
                    size="small" 
                    onClick={handleZoomToFit}
                    sx={{ 
                      color: '#7f8c8d',
                      '&:hover': {
                        backgroundColor: '#f8fafc'
                      }
                    }}
                  >
                    <CenterFocusStrong fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Stack>
              
              <Divider orientation="vertical" flexItem sx={{ mx: 1, height: 20 }} />
              
              <Tooltip title="Export">
                <IconButton 
                  size="small" 
                  onClick={handleExportPNG}
                  sx={{ color: '#7f8c8d' }}
                >
                  <Download fontSize="small" />
                </IconButton>
              </Tooltip>

              <Tooltip title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}>
                <IconButton 
                  size="small" 
                  onClick={handleFullscreen}
                  sx={{ color: '#7f8c8d' }}
                >
                  {isFullscreen ? <FullscreenExit fontSize="small" /> : <Fullscreen fontSize="small" />}
                </IconButton>
              </Tooltip>

              <Tooltip title="Close">
                <IconButton 
                  size="small" 
                  onClick={onClose}
                  sx={{ color: '#7f8c8d' }}
                >
                  <Close fontSize="small" />
                </IconButton>
              </Tooltip>
            </Stack>
          </Paper>

          {/* Canvas Area */}
          <Box sx={{ flex: 1, position: 'relative', backgroundColor: '#ffffff' }}>
            {/* Error Display */}
            {error && (
              <Alert 
                severity="error" 
                onClose={() => setError(null)}
                sx={{ 
                  position: 'absolute',
                  top: 16,
                  left: 16,
                  right: 16,
                  zIndex: 10,
                }}
              >
                {error}
              </Alert>
            )}

            {/* Canvas Component */}
            <WhiteboardCanvas
              ref={canvasRef}
              tool={tool}
              color={color}
              brushSize={brushSize}
               textFormat={textFormat}
              onDrawing={handleDrawing}
              onRemoteDrawing={onRemoteDrawing}
              meetingId={meetingId}
              isFullscreen={isFullscreen}
            />

            {/* Loading Overlay */}
            {(isLoading || hookIsLoading) && (
  <Box
    sx={{
      position: 'absolute',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(255, 255, 255, 0.7)', // More transparent
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 10,
      pointerEvents: 'none', // CRITICAL: Don't block interactions
      opacity: isLoading || hookIsLoading ? 1 : 0,
      transition: 'opacity 0.15s ease-in-out', // Smooth fade
    }}
  >
    <CircularProgress 
      size={24} // Smaller spinner
      sx={{ color: '#1976d2' }} 
    />
  </Box>
)}
          </Box>

          {/* Bottom Status Bar */}
          <Paper
            elevation={0}
            sx={{
              height: 32,
              backgroundColor: '#f8f9fa',
              borderTop: '1px solid #e1e5e9',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              px: 2,
              zIndex: 10,
            }}
          >
            <Typography variant="caption" sx={{ 
              fontSize: '11px', 
              color: '#7f8c8d' 
            }}>
              {participants.length} participant{participants.length !== 1 ? 's' : ''}
            </Typography>
            
            <Typography variant="caption" sx={{ 
              fontSize: '11px', 
              color: '#7f8c8d' 
            }}>
              Canvas • 100% • Ready
            </Typography>
          </Paper>
        </Box>

        {/* Settings Dialog */}
        <Dialog
          open={showSettings}
          onClose={() => setShowSettings(false)}
          maxWidth="sm"
          fullWidth
          PaperProps={{
            sx: {
              borderRadius: '12px',
              maxHeight: '80vh'
            }
          }}
        >
          <DialogTitle sx={{
            borderBottom: '1px solid #e1e5e9',
            backgroundColor: '#fafbfc'
          }}>
            <Stack direction="row" alignItems="center" spacing={1}>
              <Settings sx={{ fontSize: '20px', color: '#1976d2' }} />
              <Typography variant="h6" sx={{ fontSize: '16px', fontWeight: 600 }}>
                Whiteboard Settings
              </Typography>
            </Stack>
          </DialogTitle>
          
          <DialogContent sx={{ p: 3 }}>
            <Stack spacing={3}>
              {/* Canvas Settings */}
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600, color: '#2c3e50' }}>
                  Canvas Settings
                </Typography>
                <Stack spacing={2}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={showGrid}
                        onChange={(e) => setShowGrid(e.target.checked)}
                        color="primary"
                      />
                    }
                    label="Show Grid"
                  />
                  <Box>
                    <Typography variant="body2" sx={{ mb: 1, color: '#5a6c7d' }}>
                      Zoom Level: {zoomLevel}%
                    </Typography>
                    <Slider
                      value={zoomLevel}
                      onChange={(e, value) => setZoomLevel(value)}
                      min={25}
                      max={500}
                      step={25}
                      marks={[
                        { value: 25, label: '25%' },
                        { value: 100, label: '100%' },
                        { value: 200, label: '200%' },
                        { value: 500, label: '500%' }
                      ]}
                      sx={{ color: '#1976d2' }}
                    />
                  </Box>
                </Stack>
              </Box>

              {/* Collaboration Settings */}
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600, color: '#2c3e50' }}>
                  Collaboration
                </Typography>
                <Stack spacing={1}>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Participants</Typography>
                    <Chip 
                      label={participants.length} 
                      size="small" 
                      sx={{ backgroundColor: '#e3f2fd', color: '#1976d2' }} 
                    />
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Connection Status</Typography>
                    <Chip 
                      label={isOnline ? 'Online' : 'Offline'} 
                      size="small" 
                      color={isOnline ? 'success' : 'error'} 
                    />
                  </Box>
                </Stack>
              </Box>

              {/* History Information */}
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600, color: '#2c3e50' }}>
                  History & State
                </Typography>
                <Stack spacing={1}>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Total Objects</Typography>
                    <Typography variant="body2" sx={{ color: '#1976d2', fontWeight: 500 }}>
                      {history.length || 0}
                    </Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Undo Available</Typography>
                    <Typography variant="body2" sx={{ color: '#1976d2', fontWeight: 500 }}>
                      {backendState.undoCount || 0}
                    </Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Redo Available</Typography>
                    <Typography variant="body2" sx={{ color: '#1976d2', fontWeight: 500 }}>
                      {backendState.redoCount || 0}
                    </Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Current Checkpoint</Typography>
                    <Typography variant="body2" sx={{ color: '#1976d2', fontWeight: 500 }}>
                      {backendState.currentCheckpoint + 1}/{backendState.checkpoints?.length || 0}
                    </Typography>
                  </Box>
                </Stack>
              </Box>
            </Stack>
          </DialogContent>

          <DialogActions sx={{ p: 2, pt: 0 }}>
            <Button 
              onClick={() => setShowSettings(false)} 
              variant="outlined"
              sx={{ textTransform: 'none' }}
            >
              Close
            </Button>
            <Button 
              onClick={handleSave} 
              variant="contained" 
              disabled={isLoading}
              sx={{ textTransform: 'none' }}
            >
              {isLoading ? <CircularProgress size={16} sx={{ mr: 1 }} /> : null}
              Save Settings
            </Button>
          </DialogActions>
        </Dialog>

        {/* Professional Color Picker Panel */}
        {showColorPicker && (
          <Paper
            elevation={4}
            sx={{
              position: 'absolute',
              top: 80,
              left: 210,
              width: 320,
              backgroundColor: '#ffffff',
              border: '1px solid #e1e5e9',
              borderRadius: '8px',
              zIndex: 1000,
            }}
          >
            <Box sx={{ p: 2, borderBottom: '1px solid #e1e5e9' }}>
              <Typography variant="subtitle2" sx={{ 
                fontSize: '13px',
                fontWeight: 600,
                color: '#2c3e50'
              }}>
                Color Palette
              </Typography>
            </Box>
            
            <Box sx={{ p: 2 }}>
              <Box sx={{ 
                display: 'grid', 
                gridTemplateColumns: 'repeat(8, 1fr)', 
                gap: 1,
                mb: 2
              }}>
                {COLOR_PALETTE.map((colorOption) => (
                  <Box
                    key={colorOption}
                    sx={{
                      width: 24,
                      height: 24,
                      backgroundColor: colorOption,
                      border: color === colorOption ? '2px solid #1976d2' : '1px solid #e1e5e9',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      transition: 'all 0.2s ease',
                      '&:hover': {
                        transform: 'scale(1.1)',
                      },
                    }}
                    onClick={() => {
                      setColor(colorOption);
                      setShowColorPicker(false);
                    }}
                  />
                ))}
              </Box>
              
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Typography variant="caption" sx={{ 
                  fontSize: '11px', 
                  color: '#7f8c8d' 
                }}>
                  Current: {color.toUpperCase()}
                </Typography>
                <Button 
                  variant="text" 
                  size="small" 
                  onClick={() => setShowColorPicker(false)}
                  sx={{
                    textTransform: 'none',
                    fontSize: '11px',
                    color: '#7f8c8d'
                  }}
                >
                  Close
                </Button>
              </Stack>
            </Box>
          </Paper>
        )}

        {/* Notifications */}
        <Snackbar
          open={syncNotification.open}
          autoHideDuration={3000}
          onClose={() => setSyncNotification(prev => ({ ...prev, open: false }))}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        >
          <Alert
            onClose={() => setSyncNotification(prev => ({ ...prev, open: false }))}
            severity={syncNotification.type}
            sx={{ width: '100%' }}
          >
            {syncNotification.message}
          </Alert>
        </Snackbar>
      </Paper>
    </Fade>
  );
};     

export default memo(Whiteboard);