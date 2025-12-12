import React from 'react';
import {
  Box,
  ButtonGroup,
  IconButton,
  Slider,
  Typography,
  Popover,
  Grid,
  Tooltip,
  Divider,
  ToggleButton,
  ToggleButtonGroup,
  Chip,
  Paper,
  Stack,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Portal,
} from '@mui/material';
import {
  // Selection Tools
  CropFree,
  PanTool,
  OpenWith,
  
  // Drawing Tools
  Brush,
  Create,
  Edit,
  Gesture,
  
  // Shape Tools
  Rectangle,
  RadioButtonUnchecked,
  Timeline,
  ChangeHistory,
  TrendingUp,
  Star,
  Hexagon,
  
  // Advanced Shape Tools
  Pentagon,
  Category,
  Crop32,
  CropSquare,
  
  // Text Tools
  TextFields,
  Title,
  FormatSize,
  
  // Color and Style
  PaletteOutlined,
  FormatColorFill,
  BorderColor,
  LineWeight,
  
  // Action Tools
  Clear,
  Undo,
  Redo,
  Save,
  Delete,
  
  // Export Tools
  Download,
  GetApp,
  Image,
  PictureAsPdf,
  
  // View Tools
  ZoomIn,
  ZoomOut,
  Fullscreen,
  GridOn,
  Layers,
  
  // Utility
  Contrast,
  Settings,
  MoreVert,
  Palette,
  ColorLens,
  Upload,
} from '@mui/icons-material';

const WhiteboardToolbar = ({
  tool,
  onToolChange,
  color,
  onColorChange,
  brushSize,
  onBrushSizeChange,
  canUndo,
  canRedo,
  onUndo,
  onRedo,
  onClear,
  onSave,
  onExportPNG,
  onExportSVG,
  highContrast,
  onToggleHighContrast,
  isFullscreen = false,
  onImport,
  isHost = false,
  onSettings,
   textFormat,          // ✅ add these two
  setTextFormat,
}) => {
  const [colorAnchorEl, setColorAnchorEl] = React.useState(null);
  const [brushAnchorEl, setBrushAnchorEl] = React.useState(null);
  const [shapeAnchorEl, setShapeAnchorEl] = React.useState(null);
  const [exportAnchorEl, setExportAnchorEl] = React.useState(null);
  const [fillColor, setFillColor] = React.useState('#FF0000');
  const [strokeWidth, setStrokeWidth] = React.useState(2);
  const [opacity, setOpacity] = React.useState(100);
  const [customColorPickerOpen, setCustomColorPickerOpen] = React.useState(false);
  const [colorPickerPosition, setColorPickerPosition] = React.useState({ top: 0, left: 0 });

  // Enhanced color palette with better visibility - 80 colors in organized rows
  const colors = [
    // Row 1 - Black and White + Primary Colors
    '#000000', '#FFFFFF', '#FF0000', '#00FF00', '#0000FF', '#FFFF00', '#FF00FF', '#00FFFF', '#FFA500', '#800080',
    
    // Row 2 - Light Pastels  
    '#FFE6E6', '#E6F7FF', '#E6FFE6', '#FFFACD', '#F0E6FF', '#FFE6F7', '#E6FFFF', '#FFF0E6', '#F5F5DC', '#E6E6FA',
    
    // Row 3 - Medium Tones
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF', '#5F27CD', '#00D2D3', '#FF9F43',
    
    // Row 4 - Vibrant Colors
    '#EE5A24', '#0ABDE3', '#10AC84', '#F79F1F', '#A3CB38', '#FDA7DF', '#12CBC4', '#C44569', '#F8B500', '#6C5CE7',
    
    // Row 5 - Deep Colors
    '#A4B0BE', '#57606F', '#2F3542', '#FF3838', '#FF6B35', '#F7931E', '#FFD23F', '#06FFA5', '#3742FA', '#9C88FF',
    
    // Row 6 - Earth Tones
    '#8B4513', '#D2691E', '#CD853F', '#DEB887', '#F4A460', '#DAA520', '#B8860B', '#A0522D', '#BC8F8F', '#F5DEB3',
    
    // Row 7 - Professional Colors
    '#2C3E50', '#34495E', '#7F8C8D', '#95A5A6', '#BDC3C7', '#ECF0F1', '#E74C3C', '#E67E22', '#F39C12', '#F1C40F',
    
    // Row 8 - Additional Variants
    '#9B59B6', '#8E44AD', '#3498DB', '#2980B9', '#1ABC9C', '#16A085', '#27AE60', '#229954', '#D35400', '#CA6F1E'
  ];

  // Selection tools
  const selectionTools = [
    { id: 'select', icon: <CropFree />, label: 'Selection Tool (S)' },
    { id: 'hand', icon: <PanTool />, label: 'Hand Tool (H)' },
    { id: 'move', icon: <OpenWith />, label: 'Move Tool (M)' },
  ];

  // Drawing tools
  const drawingTools = [
    { id: 'pen', icon: <Brush />, label: 'Pen (P)' },
    { id: 'pencil', icon: <Create />, label: 'Pencil (Shift+P)' },
    { id: 'marker', icon: <Edit />, label: 'Marker' },
    { id: 'highlighter', icon: <Gesture />, label: 'Highlighter' },
    { id: 'eraser', icon: <Delete />, label: 'Eraser (E)' },
  ];

  // Basic shapes
  const basicShapes = [
    { id: 'line', icon: <Timeline />, label: 'Line (L)' },
    { id: 'rectangle', icon: <Rectangle />, label: 'Rectangle (R)' },
    { id: 'circle', icon: <RadioButtonUnchecked />, label: 'Circle (O)' },
    { id: 'triangle', icon: <ChangeHistory />, label: 'Triangle (T)' },
    { id: 'arrow', icon: <TrendingUp />, label: 'Arrow (A)' },
    { id: 'star', icon: <Star />, label: 'Star' },
    { id: 'square', icon: <CropSquare />, label: 'Square' },
{ id: 'ellipse', icon: <RadioButtonUnchecked />, label: 'Ellipse' },
    { id: 'text', icon: <TextFields />, label: 'Text (T)' },
  ];

  // Advanced shapes
  const advancedShapes = [
    { id: 'polygon', icon: <Hexagon />, label: 'Polygon' },
    { id: 'pentagon', icon: <Pentagon />, label: 'Pentagon' },
    { id: 'hexagon', icon: <Category />, label: 'Hexagon' },
    { id: 'rounded_rect', icon: <Crop32 />, label: 'Rounded Rectangle' },
    { id: 'square', icon: <CropSquare />, label: 'Square' },
  ];

  // Keyboard shortcuts handler
  React.useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA') return;
      
      const key = event.key.toLowerCase();
      const isShift = event.shiftKey;
      const isCtrl = event.ctrlKey || event.metaKey;
      
      if (isCtrl) {
        switch (key) {
          case 'z':
            event.preventDefault();
            if (isShift && canRedo) {
              onRedo();
            } else if (canUndo) {
              onUndo();
            }
            break;
          case 'y':
            event.preventDefault();
            if (canRedo) onRedo();
            break;
          default:
            break;
        }
        return;
      }
      
      switch (key) {
        case 'p':
          event.preventDefault();
          onToolChange(isShift ? 'pencil' : 'pen');
          break;
        case 'e':
          event.preventDefault();
          onToolChange('eraser');
          break;
        case 'r':
          event.preventDefault();
          onToolChange('rectangle');
          break;
        case 'o':
          event.preventDefault();
          onToolChange('circle');
          break;
        case 't':
          event.preventDefault();
          onToolChange('triangle');
          break;
        case 'a':
          event.preventDefault();
          onToolChange('arrow');
          break;
        case 'l':
          event.preventDefault();
          onToolChange('line');
          break;
        case 's':
          event.preventDefault();
          onToolChange('select');
          break;
        case 'h':
          event.preventDefault();
          onToolChange('hand');
          break;
        case 'm':
          event.preventDefault();
          onToolChange('marker');
          break;
        default:
          break;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onToolChange, onUndo, onRedo, canUndo, canRedo]);

  // Close color picker on outside click
  React.useEffect(() => {
    const handleClickOutside = (event) => {
      if (customColorPickerOpen && !event.target.closest('.custom-color-picker') && !event.target.closest('.color-picker-button')) {
        setCustomColorPickerOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [customColorPickerOpen]);

  // Custom color picker handler for fullscreen
  const handleCustomColorClick = (event) => {
    event.stopPropagation();
    if (isFullscreen) {
      const rect = event.currentTarget.getBoundingClientRect();
      setColorPickerPosition({
        top: rect.bottom + 10,
        left: rect.left,
      });
      setCustomColorPickerOpen(true);
    } else {
      setColorAnchorEl(event.currentTarget);
    }
  };

  const handleBrushClick = (event) => {
    setBrushAnchorEl(event.currentTarget);
  };

  const handleShapeClick = (event) => {
    setShapeAnchorEl(event.currentTarget);
  };

  const handleExportClick = (event) => {
    setExportAnchorEl(event.currentTarget);
  };

  const handleColorSelect = (selectedColor) => {
    onColorChange(selectedColor);
    setColorAnchorEl(null);
    setCustomColorPickerOpen(false);
  };

  const toolbarHeight = isFullscreen ? 120 : 90;

  // Enhanced color palette with high contrast design
  const CustomColorPicker = () => (
    <div
      className="custom-color-picker"
      style={{
        position: 'fixed',
        top: colorPickerPosition.top,
        left: Math.min(colorPickerPosition.left, window.innerWidth - 520),
        width: '500px',
        maxHeight: '70vh',
        backgroundColor: highContrast ? '#1a1a1a' : '#ffffff',
        color: highContrast ? '#ffffff' : '#000000',
        border: `3px solid ${highContrast ? '#ffffff' : '#333333'}`,
        borderRadius: '12px',
        padding: '20px',
        zIndex: 999999999,
        boxShadow: highContrast 
          ? '0 12px 40px rgba(255,255,255,0.3)' 
          : '0 12px 40px rgba(0,0,0,0.4)',
        overflow: 'auto',
      }}
    >
      <Typography 
        variant="h6" 
        style={{ 
          marginBottom: '20px', 
          color: highContrast ? '#ffffff' : '#000000',
          fontWeight: 'bold',
          textAlign: 'center'
        }}
      >
        Color Palette ({colors.length} colors)
      </Typography>
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(10, 1fr)', 
        gap: '8px',
        marginBottom: '16px'
      }}>
        {colors.map((colorOption) => (
          <div
            key={colorOption}
            style={{
              width: '36px',
              height: '36px',
              backgroundColor: colorOption,
              border: color === colorOption 
                ? `4px solid ${highContrast ? '#ffffff' : '#000000'}` 
                : `2px solid ${highContrast ? '#999999' : '#cccccc'}`,
              borderRadius: '8px',
              cursor: 'pointer',
              transition: 'all 0.2s ease',
              boxShadow: color === colorOption 
                ? `0 4px 12px rgba(${colorOption === '#000000' ? '255,255,255' : '0,0,0'},0.4)`
                : '0 2px 8px rgba(0,0,0,0.2)',
            }}
            onClick={(e) => {
              e.stopPropagation();
              handleColorSelect(colorOption);
            }}
            onMouseEnter={(e) => {
              e.target.style.transform = 'scale(1.15)';
              e.target.style.boxShadow = '0 6px 16px rgba(0,0,0,0.3)';
              e.target.style.border = `3px solid ${highContrast ? '#ffffff' : '#000000'}`;
              e.target.style.zIndex = '1';
            }}
            onMouseLeave={(e) => {
              e.target.style.transform = 'scale(1)';
              e.target.style.boxShadow = color === colorOption 
                ? `0 4px 12px rgba(${colorOption === '#000000' ? '255,255,255' : '0,0,0'},0.4)`
                : '0 2px 8px rgba(0,0,0,0.2)';
              e.target.style.border = color === colorOption 
                ? `4px solid ${highContrast ? '#ffffff' : '#000000'}` 
                : `2px solid ${highContrast ? '#999999' : '#cccccc'}`;
              e.target.style.zIndex = '0';
            }}
            title={colorOption}
          />
        ))}
      </div>
      <div style={{ 
        textAlign: 'center',
        padding: '12px',
        backgroundColor: highContrast ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.05)',
        borderRadius: '8px'
      }}>
        <Typography 
          variant="body2" 
          style={{ 
            color: highContrast ? '#cccccc' : '#666666',
            fontWeight: '500'
          }}
        >
          Selected: <strong>{color}</strong> | Total: {colors.length} colors
        </Typography>
      </div>
    </div>
  );

  return (
    <>
      <Paper
        elevation={3}
        sx={{
          p: 2,
          backgroundColor: highContrast ? '#1a1a1a' : '#f8f9fa',
          color: highContrast ? '#ffffff' : '#212529',
          minHeight: toolbarHeight,
          borderRadius: 3,
          border: highContrast ? '2px solid #ffffff' : '1px solid #e9ecef',
          zIndex: isFullscreen ? 1200 : 'auto',
        }}
        role="toolbar"
        aria-label="Whiteboard drawing tools"
      >
        <Stack spacing={3}>
          {/* Main toolbar row */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
            
            {/* Selection Tools */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography 
                variant="caption" 
                sx={{ 
                  color: highContrast ? '#cccccc' : '#6c757d', 
                  minWidth: 60,
                  fontWeight: '600',
                  fontSize: '0.8rem'
                }}
              >
                Selection
              </Typography>
              <ToggleButtonGroup
                value={tool}
                exclusive
                onChange={(e, newTool) => newTool && onToolChange(newTool)}
                size="small"
                sx={{
                  '& .MuiToggleButton-root': {
                    color: highContrast ? '#ffffff' : '#495057',
                    borderColor: highContrast ? '#999999' : '#ced4da',
                    backgroundColor: highContrast ? '#333333' : '#ffffff',
                    border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                    '&:hover': {
                      backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                      borderColor: highContrast ? '#bbbbbb' : '#007bff',
                    },
                    '&.Mui-selected': {
                      backgroundColor: highContrast ? '#ffffff' : '#007bff',
                      color: highContrast ? '#000000' : '#ffffff',
                      borderColor: highContrast ? '#ffffff' : '#007bff',
                      '&:hover': {
                        backgroundColor: highContrast ? '#f0f0f0' : '#0056b3',
                      },
                    },
                  },
                }}
              >
                {selectionTools.map((toolItem) => (
                  <ToggleButton
                    key={toolItem.id}
                    value={toolItem.id}
                    aria-label={toolItem.label}
                    title={toolItem.label}
                  >
                    {toolItem.icon}
                  </ToggleButton>
                ))}
              </ToggleButtonGroup>
            </Box>

            <Divider orientation="vertical" flexItem sx={{ 
              bgcolor: highContrast ? '#999999' : '#dee2e6',
              width: '2px'
            }} />

            {/* Drawing Tools */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography 
                variant="caption" 
                sx={{ 
                  color: highContrast ? '#cccccc' : '#6c757d', 
                  minWidth: 50,
                  fontWeight: '600',
                  fontSize: '0.8rem'
                }}
              >
                Tools
              </Typography>
              <ToggleButtonGroup
                value={tool}
                exclusive
                onChange={(e, newTool) => newTool && onToolChange(newTool)}
                size="small"
                sx={{
                  '& .MuiToggleButton-root': {
                    color: highContrast ? '#ffffff' : '#495057',
                    borderColor: highContrast ? '#999999' : '#ced4da',
                    backgroundColor: highContrast ? '#333333' : '#ffffff',
                    border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                    '&:hover': {
                      backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                      borderColor: highContrast ? '#bbbbbb' : '#007bff',
                    },
                    '&.Mui-selected': {
                      backgroundColor: highContrast ? '#ffffff' : '#007bff',
                      color: highContrast ? '#000000' : '#ffffff',
                      borderColor: highContrast ? '#ffffff' : '#007bff',
                      '&:hover': {
                        backgroundColor: highContrast ? '#f0f0f0' : '#0056b3',
                      },
                    },
                  },
                }}
              >
                {drawingTools.map((toolItem) => (
                  <ToggleButton
                    key={toolItem.id}
                    value={toolItem.id}
                    aria-label={toolItem.label}
                    title={toolItem.label}
                  >
                    {toolItem.icon}
                  </ToggleButton>
                ))}
              </ToggleButtonGroup>
            </Box>

            <Divider orientation="vertical" flexItem sx={{ 
              bgcolor: highContrast ? '#999999' : '#dee2e6',
              width: '2px'
            }} />

            {/* Brushes */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography 
                variant="caption" 
                sx={{ 
                  color: highContrast ? '#cccccc' : '#6c757d', 
                  minWidth: 50,
                  fontWeight: '600',
                  fontSize: '0.8rem'
                }}
              >
                Brushes
              </Typography>
              <IconButton
                onClick={handleBrushClick}
                sx={{ 
                  color: highContrast ? '#ffffff' : '#495057',
                  bgcolor: brushAnchorEl ? (highContrast ? '#555555' : 'rgba(0,123,255,0.1)') : (highContrast ? '#333333' : '#ffffff'),
                  border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                  '&:hover': {
                    backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.15)',
                    borderColor: highContrast ? '#bbbbbb' : '#007bff',
                  },
                }}
              >
                <LineWeight />
              </IconButton>
              
              <Menu
                anchorEl={brushAnchorEl}
                open={Boolean(brushAnchorEl)}
                onClose={() => setBrushAnchorEl(null)}
                PaperProps={{
                  sx: { 
                    bgcolor: highContrast ? '#1a1a1a' : '#ffffff', 
                    color: highContrast ? '#ffffff' : '#000000', 
                    minWidth: 250,
                    zIndex: 999999,
                    border: `2px solid ${highContrast ? '#999999' : '#e9ecef'}`,
                  }
                }}
              >
                <Box sx={{ p: 2 }}>
                  <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: '600' }}>
                    Brush Settings
                  </Typography>
                  <Stack spacing={3}>
                    <Box>
                      <Typography variant="caption" sx={{ fontWeight: '500' }}>
                        Size: {brushSize}px
                      </Typography>
                      <Slider
                        value={brushSize}
                        onChange={(e, value) => onBrushSizeChange(value)}
                        min={1}
                        max={50}
                        sx={{ 
                          color: highContrast ? '#ffffff' : '#007bff',
                          '& .MuiSlider-thumb': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                          '& .MuiSlider-track': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                        }}
                      />
                    </Box>
                    <Box>
                      <Typography variant="caption" sx={{ fontWeight: '500' }}>
                        Stroke Width: {strokeWidth}px
                      </Typography>
                      <Slider
                        value={strokeWidth}
                        onChange={(e, value) => setStrokeWidth(value)}
                        min={1}
                        max={20}
                        sx={{ 
                          color: highContrast ? '#ffffff' : '#007bff',
                          '& .MuiSlider-thumb': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                          '& .MuiSlider-track': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                        }}
                      />
                    </Box>
                    <Box>
                      <Typography variant="caption" sx={{ fontWeight: '500' }}>
                        Opacity: {opacity}%
                      </Typography>
                      <Slider
                        value={opacity}
                        onChange={(e, value) => setOpacity(value)}
                        min={10}
                        max={100}
                        sx={{ 
                          color: highContrast ? '#ffffff' : '#007bff',
                          '& .MuiSlider-thumb': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                          '& .MuiSlider-track': {
                            backgroundColor: highContrast ? '#ffffff' : '#007bff',
                          },
                        }}
                      />
                    </Box>
                  </Stack>
                </Box>
              </Menu>
            </Box>

            <Divider orientation="vertical" flexItem sx={{ 
              bgcolor: highContrast ? '#999999' : '#dee2e6',
              width: '2px'
            }} />

            {/* Shapes */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography 
                variant="caption" 
                sx={{ 
                  color: highContrast ? '#cccccc' : '#6c757d', 
                  minWidth: 50,
                  fontWeight: '600',
                  fontSize: '0.8rem'
                }}
              >
                Shapes
              </Typography>
              <ToggleButtonGroup
                value={tool}
                exclusive
                onChange={(e, newTool) => newTool && onToolChange(newTool)}
                size="small"
                sx={{
                  '& .MuiToggleButton-root': {
                    color: highContrast ? '#ffffff' : '#495057',
                    borderColor: highContrast ? '#999999' : '#ced4da',
                    backgroundColor: highContrast ? '#333333' : '#ffffff',
                    border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                    '&:hover': {
                      backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                      borderColor: highContrast ? '#bbbbbb' : '#007bff',
                    },
                    '&.Mui-selected': {
                      backgroundColor: highContrast ? '#ffffff' : '#007bff',
                      color: highContrast ? '#000000' : '#ffffff',
                      borderColor: highContrast ? '#ffffff' : '#007bff',
                      '&:hover': {
                        backgroundColor: highContrast ? '#f0f0f0' : '#0056b3',
                      },
                    },
                  },
                }}
              >
                {basicShapes.map((toolItem) => (
                  <ToggleButton
                    key={toolItem.id}
                    value={toolItem.id}
                    aria-label={toolItem.label}
                    title={toolItem.label}
                  >
                    {toolItem.icon}
                  </ToggleButton>
                ))}
              </ToggleButtonGroup>
              
              <IconButton
                onClick={handleShapeClick}
                sx={{ 
                  color: highContrast ? '#ffffff' : '#495057',
                  border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                  backgroundColor: highContrast ? '#333333' : '#ffffff',
                  '&:hover': {
                    backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                    borderColor: highContrast ? '#bbbbbb' : '#007bff',
                  },
                }}
                title="More Shapes"
              >
                <MoreVert />
              </IconButton>

              <Menu
                anchorEl={shapeAnchorEl}
                open={Boolean(shapeAnchorEl)}
                onClose={() => setShapeAnchorEl(null)}
                PaperProps={{
                  sx: { 
                    bgcolor: highContrast ? '#1a1a1a' : '#ffffff', 
                    color: highContrast ? '#ffffff' : '#000000',
                    zIndex: 999999,
                    border: `2px solid ${highContrast ? '#999999' : '#e9ecef'}`,
                  }
                }}
              >
                {advancedShapes.map((shape) => (
                  <MenuItem 
                    key={shape.id}
                    onClick={() => {
                      onToolChange(shape.id);
                      setShapeAnchorEl(null);
                    }}
                  >
                    <ListItemIcon sx={{ color: highContrast ? '#ffffff' : '#495057' }}>
                      {shape.icon}
                    </ListItemIcon>
                    <ListItemText>{shape.label}</ListItemText>
                  </MenuItem>
                ))}
              </Menu>
            </Box>

            <Divider orientation="vertical" flexItem sx={{ 
              bgcolor: highContrast ? '#999999' : '#dee2e6',
              width: '2px'
            }} />

            {/* Enhanced Colors Section */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography 
                variant="caption" 
                sx={{ 
                  color: highContrast ? '#cccccc' : '#6c757d', 
                  minWidth: 50,
                  fontWeight: '600',
                  fontSize: '0.8rem'
                }}
              >
                Colors
              </Typography>
              
              {/* Large Color Picker Button with Enhanced Visibility */}
              <Box
                className="color-picker-button"
                sx={{
                  width: isFullscreen ? 60 : 50,
                  height: isFullscreen ? 60 : 50,
                  backgroundColor: color,
                  border: `4px solid ${highContrast ? '#ffffff' : '#333333'}`,
                  borderRadius: 3,
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  position: 'relative',
                  boxShadow: highContrast 
                    ? '0 6px 20px rgba(255,255,255,0.3)' 
                    : '0 6px 20px rgba(0,0,0,0.2)',
                  '&:hover': {
                    transform: 'scale(1.1)',
                    boxShadow: highContrast 
                      ? '0 8px 25px rgba(255,255,255,0.4)' 
                      : '0 8px 25px rgba(0,0,0,0.3)',
                  },
                  transition: 'all 0.2s ease',
                }}
                onClick={handleCustomColorClick}
                title={`Current Color: ${color} - Click to see all ${colors.length} colors`}
              >
                <ColorLens 
                  sx={{ 
                    color: color === '#FFFFFF' || color === '#FFFF00' ? '#000000' : '#FFFFFF',
                    fontSize: isFullscreen ? 28 : 24,
                    filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.3))',
                  }} 
                />
              </Box>

              {/* Enhanced Quick Color Palette */}
              <Box sx={{ 
                display: 'flex', 
                gap: 1, 
                flexWrap: 'wrap', 
                maxWidth: isFullscreen ? 600 : 500,
                padding: 1,
                backgroundColor: highContrast ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.02)',
                borderRadius: 2,
                border: `1px solid ${highContrast ? '#666666' : '#e9ecef'}`,
              }}>
                {colors.slice(0, isFullscreen ? 40 : 30).map((colorOption) => (
                  <Box
                    key={colorOption}
                    sx={{
                      width: isFullscreen ? 28 : 24,
                      height: isFullscreen ? 28 : 24,
                      backgroundColor: colorOption,
                      border: color === colorOption 
                        ? `3px solid ${highContrast ? '#ffffff' : '#000000'}` 
                        : `2px solid ${highContrast ? '#999999' : '#cccccc'}`,
                      borderRadius: 2,
                      cursor: 'pointer',
                      transition: 'all 0.2s ease',
                      '&:hover': {
                        transform: 'scale(1.2)',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                        zIndex: 1,
                      },
                    }}
                    onClick={() => handleColorSelect(colorOption)}
                    title={colorOption}
                  />
                ))}
              </Box>

              {/* Standard Popover for non-fullscreen mode with enhanced styling */}
              {!isFullscreen && (
                <Popover
                  open={Boolean(colorAnchorEl)}
                  anchorEl={colorAnchorEl}
                  onClose={() => setColorAnchorEl(null)}
                  anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{
                    sx: { 
                      backgroundColor: highContrast ? '#1a1a1a' : '#ffffff',
                      color: highContrast ? '#ffffff' : '#000000',
                      zIndex: 1500,
                      border: `3px solid ${highContrast ? '#ffffff' : '#333333'}`,
                      borderRadius: 3,
                    }
                  }}
                >
                  <Box sx={{ p: 3, width: 420, backgroundColor: highContrast ? '#1a1a1a' : '#ffffff' }}>
                    <Typography variant="h6" gutterBottom sx={{ fontWeight: '600', textAlign: 'center' }}>
                      Choose Color ({colors.length} colors)
                    </Typography>
                    <Grid container spacing={1}>
                      {colors.map((colorOption) => (
                        <Grid item key={colorOption}>
                          <Box
                            sx={{
                              width: 32,
                              height: 32,
                              backgroundColor: colorOption,
                              border: color === colorOption 
                                ? `4px solid ${highContrast ? '#ffffff' : '#000000'}` 
                                : `2px solid ${highContrast ? '#999999' : '#cccccc'}`,
                              borderRadius: 2,
                              cursor: 'pointer',
                              transition: 'all 0.2s ease',
                              '&:hover': {
                                transform: 'scale(1.15)',
                                boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                                border: `3px solid ${highContrast ? '#ffffff' : '#000000'}`,
                                zIndex: 1,
                              },
                            }}
                            onClick={() => handleColorSelect(colorOption)}
                            title={colorOption}
                          />
                        </Grid>
                      ))}
                    </Grid>
                    <Box sx={{ mt: 2, textAlign: 'center' }}>
                      <Typography variant="body2" sx={{ fontWeight: '500' }}>
                        Selected: <strong>{color}</strong> | Total: {colors.length} colors
                      </Typography>
                    </Box>
                  </Box>
                </Popover>
              )}
            </Box>
    
          </Box>

          {/* Action buttons row with enhanced styling */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, justifyContent: 'space-between' }}>
            
            {/* Left side - Undo/Redo */}
            <ButtonGroup size="medium" sx={{
              '& .MuiButton-root': {
                borderColor: highContrast ? '#999999' : '#ced4da',
              }
            }}>
              <IconButton 
                onClick={onUndo} 
                disabled={!canUndo}
                sx={{ 
                  color: highContrast ? '#ffffff' : '#495057',
                  backgroundColor: highContrast ? '#333333' : '#ffffff',
                  border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                  '&:hover': {
                    backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                    borderColor: highContrast ? '#bbbbbb' : '#007bff',
                  },
                  '&:disabled': {
                    color: highContrast ? '#666666' : '#adb5bd',
                    backgroundColor: highContrast ? '#2a2a2a' : '#e9ecef',
                  },
                }}
                title="Undo (Ctrl+Z)"
              >
                <Undo />
              </IconButton>
              <IconButton 
                onClick={onRedo} 
                disabled={!canRedo}
                sx={{ 
                  color: highContrast ? '#ffffff' : '#495057',
                  backgroundColor: highContrast ? '#333333' : '#ffffff',
                  border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                  '&:hover': {
                    backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                    borderColor: highContrast ? '#bbbbbb' : '#007bff',
                  },
                  '&:disabled': {
                    color: highContrast ? '#666666' : '#adb5bd',
                    backgroundColor: highContrast ? '#2a2a2a' : '#e9ecef',
                  },
                }}
                title="Redo (Ctrl+Y)"
              >
                <Redo />
              </IconButton>
            </ButtonGroup>

            {/* Center - Current tool info with enhanced visibility */}
            <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
              <Chip
                label={`${tool.charAt(0).toUpperCase() + tool.slice(1)} | Size: ${brushSize}px`}
                size="medium"
                sx={{ 
                  backgroundColor: highContrast ? '#ffffff' : '#007bff',
                  color: highContrast ? '#000000' : '#ffffff',
                  fontWeight: '600',
                  fontSize: '0.9rem',
                  height: 36,
                }}
              />
              <Chip
                label={color}
                size="medium"
                sx={{ 
                  backgroundColor: color,
                  color: color === '#FFFFFF' || color === '#FFFF00' || color === '#00FFFF' ? '#000000' : '#ffffff',
                  border: `2px solid ${highContrast ? '#ffffff' : '#333333'}`,
                  fontWeight: '600',
                  fontSize: '0.85rem',
                  height: 36,
                }}
              />
              {highContrast && (
                <Chip
                  label="High Contrast"
                  size="medium"
                  sx={{ 
                    backgroundColor: '#ffffff', 
                    color: '#000000',
                    fontWeight: '600',
                    height: 36,
                  }}
                />
              )}
              {isFullscreen && (
                <Chip
                  label="FULLSCREEN"
                  size="medium"
                  sx={{ 
                    backgroundColor: '#ff9800', 
                    color: '#000000',
                    fontWeight: '600',
                    height: 36,
                  }}
                />
              )}
            </Box>

            {/* Right side - Actions with enhanced styling */}
            <Box sx={{ display: 'flex', gap: 2 }}>
              {isHost && onImport && (
                <IconButton 
                  onClick={() => {
                    const input = document.createElement('input');
                    input.type = 'file';
                    input.accept = 'image/*';
                    input.onchange = (e) => {
                      if (e.target.files[0]) {
                        onImport(e.target.files[0]);
                      }
                    };
                    input.click();
                  }}
                  sx={{ 
                    color: '#28a745',
                    backgroundColor: highContrast ? 'rgba(40,167,69,0.1)' : 'rgba(40,167,69,0.05)',
                    border: `2px solid ${highContrast ? '#28a745' : 'rgba(40,167,69,0.3)'}`,
                    '&:hover': {
                      backgroundColor: 'rgba(40,167,69,0.15)',
                      borderColor: '#28a745',
                    },
                  }}
                  title="Import Image (Host Only)"
                >
                  <Upload />
                </IconButton>
              )}
              
              <IconButton 
                onClick={onClear} 
                sx={{ 
                  color: '#dc3545',
                  backgroundColor: highContrast ? 'rgba(220,53,69,0.1)' : 'rgba(220,53,69,0.05)',
                  border: `2px solid ${highContrast ? '#dc3545' : 'rgba(220,53,69,0.3)'}`,
                  '&:hover': {
                    backgroundColor: 'rgba(220,53,69,0.15)',
                    borderColor: '#dc3545',
                  },
                }}
                title="Clear All"
              >
                <Clear />
              </IconButton>
              
              <IconButton 
                onClick={onSave} 
                sx={{ 
                  color: '#28a745',
                  backgroundColor: highContrast ? 'rgba(40,167,69,0.1)' : 'rgba(40,167,69,0.05)',
                  border: `2px solid ${highContrast ? '#28a745' : 'rgba(40,167,69,0.3)'}`,
                  '&:hover': {
                    backgroundColor: 'rgba(40,167,69,0.15)',
                    borderColor: '#28a745',
                  },
                }}
                title="Save"
              >
                <Save />
              </IconButton>
              
              <IconButton
                onClick={handleExportClick}
                sx={{ 
                  color: highContrast ? '#ffffff' : '#495057',
                  backgroundColor: highContrast ? '#333333' : '#ffffff',
                  border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                  '&:hover': {
                    backgroundColor: highContrast ? '#555555' : 'rgba(0,123,255,0.1)',
                    borderColor: highContrast ? '#bbbbbb' : '#007bff',
                  },
                }}
                title="Export"
              >
                <Download />
              </IconButton>
              
              <IconButton
                onClick={onToggleHighContrast}
                sx={{ 
                  color: highContrast ? '#000000' : '#495057',
                  backgroundColor: highContrast ? '#ffffff' : 'rgba(0,123,255,0.05)',
                  border: `2px solid ${highContrast ? '#ffffff' : 'rgba(0,123,255,0.3)'}`,
                  '&:hover': {
                    backgroundColor: highContrast ? '#f0f0f0' : 'rgba(0,123,255,0.15)',
                    borderColor: highContrast ? '#e6e6e6' : '#007bff',
                  },
                }}
                title="Toggle High Contrast"
              >
                <Contrast />
              </IconButton>
              
              {onSettings && (
                <IconButton
                  onClick={onSettings}
                  sx={{ 
                    color: highContrast ? '#ffffff' : '#6c757d',
                    backgroundColor: highContrast ? '#333333' : '#ffffff',
                    border: `2px solid ${highContrast ? '#999999' : '#ced4da'}`,
                    '&:hover': {
                      backgroundColor: highContrast ? '#555555' : 'rgba(108,117,125,0.1)',
                      borderColor: highContrast ? '#bbbbbb' : '#6c757d',
                    },
                  }}
                  title="Settings"
                >
                  <Settings />
                </IconButton>
              )}

              <Menu
                anchorEl={exportAnchorEl}
                open={Boolean(exportAnchorEl)}
                onClose={() => setExportAnchorEl(null)}
                PaperProps={{
                  sx: { 
                    bgcolor: highContrast ? '#1a1a1a' : '#ffffff', 
                    color: highContrast ? '#ffffff' : '#000000',
                    zIndex: 999999,
                    border: `2px solid ${highContrast ? '#999999' : '#e9ecef'}`,
                  }
                }}
              >
                <MenuItem onClick={() => { onExportPNG(); setExportAnchorEl(null); }}>
                  <ListItemIcon sx={{ color: highContrast ? '#ffffff' : '#495057' }}>
                    <Image />
                  </ListItemIcon>
                  <ListItemText>Export as PNG</ListItemText>
                </MenuItem>
                <MenuItem onClick={() => { onExportSVG(); setExportAnchorEl(null); }}>
                  <ListItemIcon sx={{ color: highContrast ? '#ffffff' : '#495057' }}>
                    <GetApp />
                  </ListItemIcon>
                  <ListItemText>Export as SVG</ListItemText>
                </MenuItem>
              </Menu>
            </Box>
          </Box>
        </Stack>
      </Paper>

      {/* Custom Color Picker for Fullscreen */}
      {isFullscreen && customColorPickerOpen && <CustomColorPicker />}
    </>
  );
};

export default WhiteboardToolbar;