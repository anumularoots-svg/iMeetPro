import React, { useState, useRef, useEffect, forwardRef, useImperativeHandle, useCallback } from 'react';
import { Box, TextField } from '@mui/material';

const WhiteboardCanvas = forwardRef(({ 
  tool, 
  color, 
  brushSize, 
  onDrawing, 
  onRemoteDrawing,
  meetingId,
  highContrast = false,
  isFullscreen = false,
  textFormat: propsTextFormat, 
}, ref) => {
  const canvasRef = useRef(null);
  const contextRef = useRef(null);
  const [isDrawing, setIsDrawing] = useState(false);
  const imageDataRef = useRef(null);
  const [canvasInitialized, setCanvasInitialized] = useState(false);
  const [previewShape, setPreviewShape] = useState(null);
  const [editingTextId, setEditingTextId] = useState(null);
  const [existingTextItems, setExistingTextItems] = useState([]);
  const [textInput, setTextInput] = useState(null);
  const [isEditingText, setIsEditingText] = useState(false);
  const [drawnShapes, setDrawnShapes] = useState([]);
  const [shapeTextInput, setShapeTextInput] = useState(null);
  const [isEditingShapeText, setIsEditingShapeText] = useState(false);
  
  const [selectionBox, setSelectionBox] = useState(null);
  const [selectedItems, setSelectedItems] = useState([]);
  const [isDragging, setIsDragging] = useState(false);
  const [selectedTextId, setSelectedTextId] = useState(null);
  const [textDragOffset, setTextDragOffset] = useState(null);
  const dragStartRef = useRef(null);
  const dragOffsetRef = useRef([]);
  
  // Store all drawings for selection
  const allDrawingsRef = useRef([]);
  
  const textFormat = propsTextFormat || {
    fontSize: 20,
    bold: false,
    italic: false,
    underline: false,
    strikethrough: false,
    fontFamily: 'Arial'
  };

  const drawingStateRef = useRef({
    isDrawing: false,
    lastPosition: { x: 0, y: 0 },
    startPosition: { x: 0, y: 0 },
    currentPath: [],
    allPaths: [],
    smoothingBuffer: []
  });

  const performanceRef = useRef({
    lastDrawTime: 0,
    frameCount: 0,
    skipFrames: false
  });

const drawSelectionBoxes = useCallback(() => {
  const canvas = canvasRef.current;
  const context = contextRef.current;
  if (!canvas || !context || selectedItems.length === 0 || tool !== 'select') return;
  
  selectedItems.forEach(item => {
    const bounds = getShapeBounds(item);
    if (!bounds) return;
    
    // CRITICAL: Use canvas coordinates directly - NO conversion needed
    const x = bounds.x;
    const y = bounds.y;
    const width = bounds.width;
    const height = bounds.height;
    
    context.save();
    context.strokeStyle = '#1976d2';
    context.lineWidth = 2 / (window.devicePixelRatio || 1);
    context.setLineDash([5, 5]);
    context.strokeRect(x, y, width, height);
    
    // Draw corner handles
    const handleSize = 8 / (window.devicePixelRatio || 1);
    const corners = [
      [x, y],
      [x + width, y],
      [x, y + height],
      [x + width, y + height]
    ];
    
    context.fillStyle = '#ffffff';
    context.setLineDash([]);
    corners.forEach(([cx, cy]) => {
      context.fillRect(cx - handleSize/2, cy - handleSize/2, handleSize, handleSize);
      context.strokeRect(cx - handleSize/2, cy - handleSize/2, handleSize, handleSize);
    });
    
    context.restore();
  });
}, [selectedItems, tool]);

  // ============ SELECTION HELPER FUNCTIONS ============
  const isPointInShape = useCallback((point, shape) => {
    if (!shape || !shape.drawing_data) return false;
    
    const data = shape.drawing_data;
    
    if (data.type === 'path' && Array.isArray(data.points)) {
      const tolerance = (shape.stroke_width || 2) + 10;
      return data.points.some(p => {
        const distance = Math.sqrt(
          Math.pow(point.x - p.x, 2) + Math.pow(point.y - p.y, 2)
        );
        return distance <= tolerance;
      });
    }
    
    if (data.type === 'shape' && data.start && data.end) {
      const minX = Math.min(data.start.x, data.end.x) - 5;
      const maxX = Math.max(data.start.x, data.end.x) + 5;
      const minY = Math.min(data.start.y, data.end.y) - 5;
      const maxY = Math.max(data.start.y, data.end.y) + 5;
      
      return point.x >= minX && point.x <= maxX && 
             point.y >= minY && point.y <= maxY;
    }
    
    if (data.type === 'text' || shape.tool_type === 'text') {
      const textPos = data.position || { x: shape.x, y: shape.y };
      const text = shape.text_content || data.text || '';
      const fontSize = shape.fontSize || textFormat.fontSize;
      const approxWidth = text.length * fontSize * 0.6;
      const approxHeight = fontSize * 1.5;
      
      return point.x >= textPos.x && point.x <= textPos.x + approxWidth &&
             point.y >= textPos.y && point.y <= textPos.y + approxHeight;
    }
    
    return false;
  }, [textFormat.fontSize]);

  const getShapeBounds = useCallback((shape) => {
    if (!shape || !shape.drawing_data) return null;
    
    const data = shape.drawing_data;
    
    if (data.type === 'path' && Array.isArray(data.points) && data.points.length > 0) {
      let minX = data.points[0].x, maxX = data.points[0].x;
      let minY = data.points[0].y, maxY = data.points[0].y;
      
      data.points.forEach(p => {
        minX = Math.min(minX, p.x);
        maxX = Math.max(maxX, p.x);
        minY = Math.min(minY, p.y);
        maxY = Math.max(maxY, p.y);
      });
      
      const padding = (shape.stroke_width || 2) + 5;
      return {
        x: minX - padding,
        y: minY - padding,
        width: maxX - minX + padding * 2,
        height: maxY - minY + padding * 2
      };
    }
    
    if (data.type === 'shape' && data.start && data.end) {
      const minX = Math.min(data.start.x, data.end.x);
      const maxX = Math.max(data.start.x, data.end.x);
      const minY = Math.min(data.start.y, data.end.y);
      const maxY = Math.max(data.start.y, data.end.y);
      
      const padding = 5;
      return {
        x: minX - padding,
        y: minY - padding,
        width: maxX - minX + padding * 2,
        height: maxY - minY + padding * 2
      };
    }
    
    if (data.type === 'text' || shape.tool_type === 'text') {
      const textPos = data.position || { x: shape.x, y: shape.y };
      const text = shape.text_content || data.text || '';
      const fontSize = shape.fontSize || textFormat.fontSize;
      const approxWidth = text.length * fontSize * 0.6;
      const approxHeight = fontSize * 1.5;
      
      return {
        x: textPos.x,
        y: textPos.y,
        width: approxWidth,
        height: approxHeight
      };
    }
    
    return null;
  }, [textFormat.fontSize]);

  const findItemsInBox = useCallback((box) => {
    const minX = Math.min(box.start.x, box.end.x);
    const maxX = Math.max(box.start.x, box.end.x);
    const minY = Math.min(box.start.y, box.end.y);
    const maxY = Math.max(box.start.y, box.end.y);
    
    return allDrawingsRef.current.filter(item => {
      const bounds = getShapeBounds(item);
      if (!bounds) return false;
      
      return bounds.x >= minX && 
             bounds.x + bounds.width <= maxX &&
             bounds.y >= minY && 
             bounds.y + bounds.height <= maxY;
    });
  }, [getShapeBounds]);

  const deleteSelectedItems = useCallback(() => {
    if (selectedItems.length === 0) return;
    
    // Create a set of IDs to delete
    const idsToDelete = new Set(selectedItems.map(item => item.drawing_id));
    
    // Remove from ref
    allDrawingsRef.current = allDrawingsRef.current.filter(
      item => !idsToDelete.has(item.drawing_id)
    );
    
    // Notify parent for undo/redo
    if (onDrawing) {
      onDrawing({
        type: 'bulk_delete',
        items: selectedItems,
        meetingId
      });
    }
    
    setSelectedItems([]);
    redrawCanvas();
  }, [selectedItems, onDrawing, meetingId]);

  const redrawCanvas = useCallback(() => {
    const canvas = canvasRef.current;
    const context = contextRef.current;
    
    if (!canvas || !context) return;
    
    context.clearRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = highContrast ? '#000000' : '#ffffff';
    context.fillRect(0, 0, canvas.width, canvas.height);
    
    // Redraw all items
    allDrawingsRef.current.forEach((item) => {
      try {
        const drawingData = item.drawing_data || {};
        
        context.save();
        context.strokeStyle = item.stroke_color || '#000000';
        context.lineWidth = item.stroke_width || 2;
        context.lineCap = 'round';
        context.lineJoin = 'round';
        context.globalAlpha = item.opacity || 1.0;
        
        if (item.tool_type === 'eraser') {
          context.globalCompositeOperation = 'destination-out';
        } else {
          context.globalCompositeOperation = 'source-over';
        }

        if (drawingData.type === 'path' && Array.isArray(drawingData.points) && drawingData.points.length > 0) {
          context.beginPath();
          context.moveTo(drawingData.points[0].x, drawingData.points[0].y);
          for (let i = 1; i < drawingData.points.length; i++) {
            context.lineTo(drawingData.points[i].x, drawingData.points[i].y);
          }
          context.stroke();
        } else if (drawingData.type === 'shape' && drawingData.start && drawingData.end) {
          drawShape(context, item.tool_type, drawingData.start, drawingData.end, item.stroke_color, item.stroke_width);
        } else if (drawingData.type === 'text' || item.tool_type === 'text') {
          const text = item.text_content || drawingData.text || '';
          const position = drawingData.position || { x: item.x, y: item.y };
          drawTextOnCanvas(context, text, position, item.stroke_color);
        }
        
        context.restore();
      } catch (error) {
        console.error('Error redrawing item:', error);
      }
    });
    
    // Draw selection boxes DIRECTLY on canvas (not as HTML overlay)
    if (selectedItems.length > 0 && tool === 'select') {
      selectedItems.forEach(item => {
        const bounds = getShapeBounds(item);
        if (bounds) {
          context.save();
          context.strokeStyle = '#1976d2';
          context.lineWidth = 2;
          context.setLineDash([5, 5]);
          context.strokeRect(bounds.x, bounds.y, bounds.width, bounds.height);
          
          // Draw corner handles
          const handleSize = 8;
          const corners = [
            { x: bounds.x, y: bounds.y },
            { x: bounds.x + bounds.width, y: bounds.y },
            { x: bounds.x, y: bounds.y + bounds.height },
            { x: bounds.x + bounds.width, y: bounds.y + bounds.height },
          ];
          
          context.fillStyle = '#ffffff';
          context.strokeStyle = '#1976d2';
          context.setLineDash([]);
          corners.forEach(corner => {
            context.fillRect(corner.x - handleSize / 2, corner.y - handleSize / 2, handleSize, handleSize);
            context.strokeRect(corner.x - handleSize / 2, corner.y - handleSize / 2, handleSize, handleSize);
          });
          
          context.restore();
        }
      });
    }
  }, [selectedItems, tool, getShapeBounds, highContrast]);

  // ✅ CRITICAL: redrawFromData for undo/redo
  const redrawFromData = useCallback((drawings) => {
    const canvas = canvasRef.current;
    const context = contextRef.current;
    
    if (!canvas || !context || !Array.isArray(drawings)) {
      console.warn('⚠️ Cannot redraw: invalid canvas or drawings');
      return;
    }

    console.log(`🎨 Redrawing ${drawings.length} items from undo/redo`);

    // Update local ref
    allDrawingsRef.current = [...drawings];
    
    // Clear selection
    setSelectedItems([]);

    // Clear and redraw
    context.clearRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = highContrast ? '#000000' : '#ffffff';
    context.fillRect(0, 0, canvas.width, canvas.height);

    drawings.forEach((drawing, index) => {
      try {
        const drawingData = drawing.drawing_data || {};
        
        context.strokeStyle = drawing.stroke_color || '#000000';
        context.lineWidth = drawing.stroke_width || 2;
        context.lineCap = 'round';
        context.lineJoin = 'round';
        context.globalAlpha = drawing.opacity || 1.0;
        
        if (drawing.tool_type === 'eraser') {
          context.globalCompositeOperation = 'destination-out';
        } else {
          context.globalCompositeOperation = 'source-over';
        }

        if (drawingData.type === 'path' && Array.isArray(drawingData.points) && drawingData.points.length > 0) {
          context.beginPath();
          context.moveTo(drawingData.points[0].x, drawingData.points[0].y);
          for (let i = 1; i < drawingData.points.length; i++) {
            context.lineTo(drawingData.points[i].x, drawingData.points[i].y);
          }
          context.stroke();
        } else if (drawingData.type === 'shape' && drawingData.start && drawingData.end) {
          drawShape(context, drawing.tool_type, drawingData.start, drawingData.end, drawing.stroke_color, drawing.stroke_width);
        } else if (drawingData.type === 'text' || drawing.tool_type === 'text') {
          const text = drawing.text_content || drawingData.text || '';
          const position = { x: drawing.x || drawingData.position?.x || 0, y: drawing.y || drawingData.position?.y || 0 };
          drawTextOnCanvas(context, text, position, drawing.stroke_color);
        }
      } catch (error) {
        console.error(`❌ Error redrawing item ${index}:`, error);
      }
    });

    console.log('✅ Redraw complete');
  }, [highContrast, drawSelectionBoxes]);

  const drawTextOnCanvas = useCallback((context, text, position, textColor) => {
    if (!context || !text) return;

    context.save();
    
    const style = `${textFormat.italic ? 'italic ' : ''}${textFormat.bold ? 'bold ' : ''}${textFormat.fontSize}px ${textFormat.fontFamily}`.trim();
    context.font = style;
    context.fillStyle = textColor || color;
    context.textBaseline = 'top';

    const lines = text.split('\n');
    const lineHeight = textFormat.fontSize * 1.2;

    lines.forEach((line, index) => {
      const yPosition = position.y + (index * lineHeight);
      context.fillText(line, position.x, yPosition);

      const textWidth = context.measureText(line).width;
      
      if (textFormat.underline) {
        const lineY = yPosition + textFormat.fontSize + 2;
        context.beginPath();
        context.moveTo(position.x, lineY);
        context.lineTo(position.x + textWidth, lineY);
        context.lineWidth = Math.max(1, textFormat.fontSize / 16);
        context.strokeStyle = textColor || color;
        context.stroke();
      }

      if (textFormat.strikethrough) {
        const lineY = yPosition + (textFormat.fontSize / 2);
        context.beginPath();
        context.moveTo(position.x, lineY);
        context.lineTo(position.x + textWidth, lineY);
        context.lineWidth = Math.max(1, textFormat.fontSize / 16);
        context.strokeStyle = textColor || color;
        context.stroke();
      }
    });

    context.restore();
  }, [color, textFormat]);

  const drawText = useCallback((text, position, isShapeText = false) => {
    const context = contextRef.current;
    if (!context || !text) return;

    context.save();
    
    const style = `${textFormat.italic ? 'italic ' : ''}${textFormat.bold ? 'bold ' : ''}${textFormat.fontSize}px ${textFormat.fontFamily}`.trim();
    context.font = style;
    context.fillStyle = color;
    context.textBaseline = 'top';

    const lines = text.split('\n');
    const lineHeight = textFormat.fontSize * 1.2;

    lines.forEach((line, index) => {
      const yPosition = position.y + (index * lineHeight);
      context.fillText(line, position.x, yPosition);

      const textWidth = context.measureText(line).width;
      
      if (textFormat.underline) {
        const lineY = yPosition + textFormat.fontSize + 2;
        context.beginPath();
        context.moveTo(position.x, lineY);
        context.lineTo(position.x + textWidth, lineY);
        context.lineWidth = Math.max(1, textFormat.fontSize / 16);
        context.strokeStyle = color;
        context.stroke();
      }

      if (textFormat.strikethrough) {
        const lineY = yPosition + (textFormat.fontSize / 2);
        context.beginPath();
        context.moveTo(position.x, lineY);
        context.lineTo(position.x + textWidth, lineY);
        context.lineWidth = Math.max(1, textFormat.fontSize / 16);
        context.strokeStyle = color;
        context.stroke();
      }
    });

    context.restore();

    if (onDrawing && !isShapeText) {
      onDrawing({
        type: 'text',
        text,
        position,
        font: style,
        color,
        underline: textFormat.underline,
        strikethrough: textFormat.strikethrough,
        bold: textFormat.bold,
        italic: textFormat.italic,
        fontSize: textFormat.fontSize,
        fontFamily: textFormat.fontFamily,
        lineHeight: lineHeight,
        meetingId,
      });
    }
  }, [color, textFormat, onDrawing, meetingId]);

  const drawSmoothLine = useCallback((context, from, to) => {
    const midPoint = {
      x: (from.x + to.x) / 2,
      y: (from.y + to.y) / 2
    };
    context.quadraticCurveTo(from.x, from.y, midPoint.x, midPoint.y);
  }, []);

  const performDraw = useCallback((coordinates) => {
    const context = contextRef.current;
    if (!context || !drawingStateRef.current.isDrawing) return;

    const now = performance.now();
    const timeDelta = now - performanceRef.current.lastDrawTime;
    
    if (timeDelta < 8) {
      performanceRef.current.skipFrames = true;
      return;
    }

    performanceRef.current.lastDrawTime = now;
    performanceRef.current.skipFrames = false;

    const lastPos = drawingStateRef.current.lastPosition;
    const distance = Math.sqrt(
      Math.pow(coordinates.x - lastPos.x, 2) + 
      Math.pow(coordinates.y - lastPos.y, 2)
    );

    if (distance < 1 && tool !== 'eraser') return;

    switch (tool) {
      case 'pen':
      case 'pencil':
      case 'marker':
      case 'highlighter':
        if (drawingStateRef.current.smoothingBuffer.length > 0) {
          const prevPoint = drawingStateRef.current.smoothingBuffer[drawingStateRef.current.smoothingBuffer.length - 1];
          drawSmoothLine(context, prevPoint, coordinates);
        } else {
          context.moveTo(coordinates.x, coordinates.y);
        }
        context.stroke();
        break;

      case 'eraser':
        context.lineTo(coordinates.x, coordinates.y);
        context.stroke();
        break;
    }

    drawingStateRef.current.smoothingBuffer.push(coordinates);
    if (drawingStateRef.current.smoothingBuffer.length > 3) {
      drawingStateRef.current.smoothingBuffer.shift();
    }

    drawingStateRef.current.currentPath.push(coordinates);
    drawingStateRef.current.lastPosition = coordinates;
  }, [tool, drawSmoothLine]);

  const getCanvasCoordinates = useCallback((event) => {
    const canvas = canvasRef.current;
    if (!canvas) return { x: 0, y: 0 };
    
    const rect = canvas.getBoundingClientRect();
    
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;

    let clientX, clientY;
    
    if (event.touches && event.touches.length > 0) {
      clientX = event.touches[0].clientX;
      clientY = event.touches[0].clientY;
    } else if (event.changedTouches && event.changedTouches.length > 0) {
      clientX = event.changedTouches[0].clientX;
      clientY = event.changedTouches[0].clientY;
    } else {
      clientX = event.clientX;
      clientY = event.clientY;
    }

    const scrollX = window.pageXOffset || document.documentElement.scrollLeft;
    const scrollY = window.pageYOffset || document.documentElement.scrollTop;

    const x = (clientX - rect.left + scrollX) * scaleX;
    const y = (clientY - rect.top + scrollY) * scaleY;

    return { x, y };
  }, []);

  const findShapeAtPosition = useCallback((x, y) => {
    for (let i = drawnShapes.length - 1; i >= 0; i--) {
      const shape = drawnShapes[i];
      const minX = Math.min(shape.start.x, shape.end.x);
      const maxX = Math.max(shape.start.x, shape.end.x);
      const minY = Math.min(shape.start.y, shape.end.y);
      const maxY = Math.max(shape.start.y, shape.end.y);
      
      if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
        return shape;
      }
    }
    return null;
  }, [drawnShapes]);

  const broadcastDrawing = useCallback((drawingData) => {
    performanceRef.current.frameCount++;
    if (performanceRef.current.frameCount % 3 !== 0) return;
    
    if (onDrawing) {
      onDrawing(drawingData);
    }
  }, [onDrawing]);

  const findTextAtPosition = useCallback((x, y) => {
    const context = contextRef.current;
    if (!context) return null;

    // ✅ STEP 1: First check existingTextItems (newly added text in current session)
    for (let i = existingTextItems.length - 1; i >= 0; i--) {
      const item = existingTextItems[i];
      
      context.font = `${item.italic ? 'italic ' : ''}${item.bold ? 'bold ' : ''}${item.fontSize || 20}px ${item.fontFamily || 'Arial'}`;
      const lines = item.text.split('\n');
      const lineHeight = (item.fontSize || 20) * 1.2;
      
      let maxWidth = 0;
      lines.forEach(line => {
        const width = context.measureText(line).width;
        if (width > maxWidth) maxWidth = width;
      });
      
      const textHeight = lines.length * lineHeight;
      const padding = 8;
      
      if (
        x >= item.position.x - padding &&
        x <= item.position.x + maxWidth + padding &&
        y >= item.position.y - padding &&
        y <= item.position.y + textHeight + padding
      ) {
        return { ...item, index: i, source: 'existingTextItems' };
      }
    }

    // ✅ STEP 2: Check allDrawingsRef for text items (loaded from backend, undo/redo, etc.)
    for (let i = allDrawingsRef.current.length - 1; i >= 0; i--) {
      const drawing = allDrawingsRef.current[i];
      
      // Check if this is a text drawing
      if (drawing.tool_type !== 'text' && drawing.drawing_data?.type !== 'text') {
        continue;
      }
      
      const drawingData = drawing.drawing_data || {};
      const text = drawing.text_content || drawingData.text || '';
      
      if (!text) continue;
      
      // Get position from drawing data
      const position = drawingData.position || { 
        x: drawing.x || 0, 
        y: drawing.y || 0 
      };
      
      // Get text formatting
      const fontSize = drawing.fontSize || drawingData.fontSize || textFormat.fontSize || 20;
      const fontFamily = drawing.fontFamily || drawingData.fontFamily || textFormat.fontFamily || 'Arial';
      const bold = drawing.bold || drawingData.bold || false;
      const italic = drawing.italic || drawingData.italic || false;
      
      // Set font for measurement
      context.font = `${italic ? 'italic ' : ''}${bold ? 'bold ' : ''}${fontSize}px ${fontFamily}`;
      
      const lines = text.split('\n');
      const lineHeight = fontSize * 1.2;
      
      let maxWidth = 0;
      lines.forEach(line => {
        const width = context.measureText(line).width;
        if (width > maxWidth) maxWidth = width;
      });
      
      const textHeight = lines.length * lineHeight;
      const padding = 8;
      
      // Check if click is within text bounds
      if (
        x >= position.x - padding &&
        x <= position.x + maxWidth + padding &&
        y >= position.y - padding &&
        y <= position.y + textHeight + padding
      ) {
        // ✅ Return a normalized text item structure for editing
        return {
          id: drawing.drawing_id || `text_${i}`,
          text: text,
          position: position,
          fontSize: fontSize,
          fontFamily: fontFamily,
          bold: bold,
          italic: italic,
          underline: drawing.underline || drawingData.underline || false,
          strikethrough: drawing.strikethrough || drawingData.strikethrough || false,
          color: drawing.stroke_color || drawingData.color || '#000000',
          index: i,
          source: 'allDrawingsRef',
          originalDrawing: drawing  // Keep reference to original for updates
        };
      }
    }

    return null;
  }, [existingTextItems, textFormat]);

  const startDrawing = useCallback((event) => {
    event.preventDefault();
    
    const coordinates = getCanvasCoordinates(event);
    const context = contextRef.current;
    if (!context) return;

    console.log('🎯 Start drawing at:', coordinates, 'Tool:', tool);

    if (tool === 'text') {
      const existingText = findTextAtPosition(coordinates.x, coordinates.y);
      
      if (existingText) {
        console.log('📝 Found existing text for editing:', existingText);
        setTextInput({ 
          x: existingText.position.x, 
          y: existingText.position.y, 
          value: existingText.text,
          id: existingText.id,
          isEditing: true,
          source: existingText.source  // ✅ Pass source info
        });
        setEditingTextId(existingText.id);
        setIsEditingText(true);
        setSelectedTextId(null);
      } else {
        setSelectedTextId(null);
        
        console.log('📝 Creating new text at exact position:', coordinates);
        setTextInput({ 
          x: coordinates.x, 
          y: coordinates.y, 
          value: '',
          id: `text_${Date.now()}`,
          isEditing: false,
          source: null
        });
        setEditingTextId(null);
        setIsEditingText(true);
      }
      return;
    }

    // ============ SELECTION TOOL ============
    if (tool === 'select') {
      // Check if clicking on selected item
      const clickedSelected = selectedItems.find(item => isPointInShape(coordinates, item));
      
      if (clickedSelected) {
        setIsDragging(true);
        dragStartRef.current = coordinates;
        
        dragOffsetRef.current = selectedItems.map(item => {
          const bounds = getShapeBounds(item);
          return bounds ? {
            x: coordinates.x - bounds.x,
            y: coordinates.y - bounds.y
          } : { x: 0, y: 0 };
        });
      } else {
        // Check if clicking new item
        const clickedItem = allDrawingsRef.current.find(item => isPointInShape(coordinates, item));
        
        if (clickedItem) {
          setSelectedItems([clickedItem]);
          setIsDragging(true);
          dragStartRef.current = coordinates;
          
          const bounds = getShapeBounds(clickedItem);
          dragOffsetRef.current = [bounds ? {
            x: coordinates.x - bounds.x,
            y: coordinates.y - bounds.y
          } : { x: 0, y: 0 }];
        } else {
          // Start selection box
          dragStartRef.current = coordinates;
          setSelectionBox({
            start: coordinates,
            end: coordinates
          });
          setSelectedItems([]);
        }
      }
      setIsDrawing(true);
      
      // Store canvas state for drawing selection box
      imageDataRef.current = context.getImageData(0, 0, context.canvas.width, context.canvas.height);
      return;
    }

    drawingStateRef.current.isDrawing = true;
    drawingStateRef.current.lastPosition = coordinates;
    drawingStateRef.current.startPosition = coordinates;
    drawingStateRef.current.currentPath = [coordinates];
    drawingStateRef.current.smoothingBuffer = [coordinates];
    
    setIsDrawing(true);
    
    performanceRef.current.lastDrawTime = performance.now();
    performanceRef.current.frameCount = 0;

    if (['line', 'rectangle', 'square', 'circle', 'ellipse', 'triangle', 'arrow'].includes(tool)) {
      imageDataRef.current = context.getImageData(0, 0, context.canvas.width, context.canvas.height);
      console.log('📐 Shape started at:', coordinates);
    }
    
    const isErase = tool === 'eraser';
    const isHighlight = tool === 'highlighter';
    
    context.strokeStyle = isErase ? '#FFFFFF' : color;
    context.lineWidth = isErase ? brushSize * 2 : (tool === 'marker' ? brushSize * 1.5 : brushSize);
    context.globalCompositeOperation = isErase ? 'destination-out' : 'source-over';
    context.globalAlpha = isHighlight ? 0.3 : 1.0;
    context.lineCap = 'round';
    context.lineJoin = 'round';
    
    if (['pen', 'pencil', 'eraser', 'marker', 'highlighter'].includes(tool)) {
      context.beginPath();
      context.moveTo(coordinates.x, coordinates.y);
    }
  }, [tool, color, brushSize, getCanvasCoordinates, findTextAtPosition, selectedItems, isPointInShape, getShapeBounds]);

  const draw = useCallback((event) => {
    if (!drawingStateRef.current.isDrawing && !isDrawing) return;
    event.preventDefault();

    const coordinates = getCanvasCoordinates(event);
    const context = contextRef.current;
    if (!context) return;

    // ============ SELECTION TOOL DRAG ============
    if (tool === 'select') {
      if (isDragging && dragStartRef.current) {
        const dx = coordinates.x - dragStartRef.current.x;
        const dy = coordinates.y - dragStartRef.current.y;
        
        dragStartRef.current = coordinates;
        
        const updatedItems = selectedItems.map((item) => {
          const newItem = JSON.parse(JSON.stringify(item));
          
          if (newItem.drawing_data.type === 'path') {
            newItem.drawing_data.points = newItem.drawing_data.points.map(p => ({
              x: p.x + dx,
              y: p.y + dy
            }));
          } else if (newItem.drawing_data.type === 'shape') {
            newItem.drawing_data.start = {
              x: newItem.drawing_data.start.x + dx,
              y: newItem.drawing_data.start.y + dy
            };
            newItem.drawing_data.end = {
              x: newItem.drawing_data.end.x + dx,
              y: newItem.drawing_data.end.y + dy
            };
          } else if (newItem.drawing_data.type === 'text' || newItem.tool_type === 'text') {
            newItem.drawing_data.position = {
              x: newItem.drawing_data.position.x + dx,
              y: newItem.drawing_data.position.y + dy
            };
          }
          
          return newItem;
        });
        
        setSelectedItems(updatedItems);
        
        // Update in main ref
        allDrawingsRef.current = allDrawingsRef.current.map(item => {
          const selected = selectedItems.find(s => s.drawing_id === item.drawing_id);
          if (selected) {
            const updated = updatedItems.find(u => u.drawing_id === item.drawing_id);
            return updated || item;
          }
          return item;
        });
        
        redrawCanvas();
      } else if (dragStartRef.current) {
        // Drawing selection box
        setSelectionBox({
          start: dragStartRef.current,
          end: coordinates
        });
        
        // Redraw with selection box
        if (imageDataRef.current) {
          context.putImageData(imageDataRef.current, 0, 0);
        }
        
        context.save();
        context.strokeStyle = '#1976d2';
        context.lineWidth = 2;
        context.setLineDash([5, 5]);
        context.fillStyle = 'rgba(25, 118, 210, 0.1)';
        const width = coordinates.x - dragStartRef.current.x;
        const height = coordinates.y - dragStartRef.current.y;
        context.fillRect(dragStartRef.current.x, dragStartRef.current.y, width, height);
        context.strokeRect(dragStartRef.current.x, dragStartRef.current.y, width, height);
        context.restore();
      }
      return;
    }

    switch (tool) {
      case 'pen':
      case 'pencil':
      case 'eraser':
      case 'marker':
      case 'highlighter':
        performDraw(coordinates);
        
        broadcastDrawing({
          type: 'draw',
          tool,
          color,
          brushSize,
          from: drawingStateRef.current.lastPosition,
          to: coordinates,
          meetingId,
        });
        break;

      case 'line':
      case 'rectangle':
      case 'square':
      case 'circle':
      case 'ellipse':
      case 'triangle':
      case 'arrow':
        setPreviewShape({ 
          start: drawingStateRef.current.startPosition, 
          end: coordinates, 
          tool,
          color,
          lineWidth: brushSize 
        });
        
        if (imageDataRef.current) {
          context.putImageData(imageDataRef.current, 0, 0);
        }
        drawShape(context, tool, drawingStateRef.current.startPosition, coordinates, color, brushSize);
        break;
    }
  }, [tool, color, brushSize, getCanvasCoordinates, performDraw, broadcastDrawing, meetingId, isDrawing, isDragging, selectedItems, redrawCanvas]);

  const stopDrawing = useCallback((event) => {
    if (!drawingStateRef.current.isDrawing && tool !== 'select' && !isDrawing) return;
    event?.preventDefault();

    const coordinates = getCanvasCoordinates(event);

    // ============ SELECTION TOOL STOP ============
    if (tool === 'select') {
      if (isDragging) {
        setIsDragging(false);
        dragStartRef.current = null;
        dragOffsetRef.current = [];
        
        // Notify parent about position changes
        if (onDrawing && selectedItems.length > 0) {
          onDrawing({
            type: 'bulk_update',
            items: selectedItems,
            meetingId
          });
        }
      } else if (selectionBox) {
        const selected = findItemsInBox(selectionBox);
        setSelectedItems(selected);
        setSelectionBox(null);
        dragStartRef.current = null;
        
        redrawCanvas();
      }
      
      setIsDrawing(false);
      drawingStateRef.current.isDrawing = false;
      return;
    }

    const isShapeTool = ['line', 'rectangle', 'square', 'circle', 'ellipse', 'triangle', 'arrow'].includes(tool);
    const isFreehandTool = ['pen', 'pencil', 'marker', 'highlighter', 'eraser'].includes(tool);
    
    if (isShapeTool && drawingStateRef.current.startPosition && coordinates) {
      const newShape = {
        id: `shape_${Date.now()}`,
        type: tool,
        start: drawingStateRef.current.startPosition,
        end: coordinates,
        color: color,
        brushSize: brushSize
      };
      setDrawnShapes(prev => [...prev, newShape]);
    }

    setPreviewShape(null);
    
    const allPoints = [...drawingStateRef.current.currentPath];
    if (coordinates && allPoints.length > 0) {
      const lastPoint = allPoints[allPoints.length - 1];
      if (!lastPoint || lastPoint.x !== coordinates.x || lastPoint.y !== coordinates.y) {
        allPoints.push(coordinates);
      }
    }
    
    console.log('🎨 Drawing completed:', {
      tool,
      isFreehand: isFreehandTool,
      isShape: isShapeTool,
      totalPoints: allPoints.length,
      pathsCount: drawingStateRef.current.allPaths.length
    });
    
    drawingStateRef.current.isDrawing = false;
    drawingStateRef.current.allPaths.push([...allPoints]);
    drawingStateRef.current.currentPath = [];
    drawingStateRef.current.smoothingBuffer = [];
    
    setIsDrawing(false);

    if (onDrawing && ((isFreehandTool && allPoints.length > 0) || (isShapeTool && drawingStateRef.current.startPosition && coordinates))) {
      const drawingId = `draw_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      const drawingData = {
        drawing_id: drawingId,
        type: isShapeTool ? 'shape' : 'path',
        tool,
        tool_type: tool,
        color,
        stroke_color: color,
        brushSize,
        stroke_width: brushSize,
        opacity: tool === 'highlighter' ? 0.3 : 1.0,
        meetingId,
        timestamp: Date.now()
      };
      
      if (isShapeTool) {
        drawingData.start = drawingStateRef.current.startPosition;
        drawingData.end = coordinates;
        drawingData.drawing_data = {
          type: 'shape',
          start: drawingStateRef.current.startPosition,
          end: coordinates
        };
        
        allDrawingsRef.current.push(drawingData);
      } else if (isFreehandTool) {
        drawingData.points = allPoints;
        drawingData.drawing_data = {
          type: 'path',
          points: allPoints
        };
        
        allDrawingsRef.current.push(drawingData);
        
        console.log('📤 Sending freehand path with', allPoints.length, 'points');
      }
      
      console.log('📤 Final drawing data:', {
        id: drawingId,
        tool: drawingData.tool,
        type: drawingData.type,
        pointsCount: drawingData.points?.length || 0,
        hasDrawingData: !!drawingData.drawing_data
      });
      
      onDrawing(drawingData);
    }
  }, [tool, color, brushSize, onDrawing, meetingId, getCanvasCoordinates, selectionBox, findItemsInBox, isDragging, selectedItems, isDrawing, redrawCanvas]);

  const handleCanvasDoubleClick = useCallback((event) => {
    event.preventDefault();
    const coordinates = getCanvasCoordinates(event);
    
    const shape = findShapeAtPosition(coordinates.x, coordinates.y);
    
    if (shape) {
      const centerX = (shape.start.x + shape.end.x) / 2;
      const centerY = (shape.start.y + shape.end.y) / 2;
      
      setShapeTextInput({
        x: centerX - 50,
        y: centerY - 10,
        shapeId: shape.id,
        value: shape.text || ''
      });
      setIsEditingShapeText(true);
    }
  }, [getCanvasCoordinates, findShapeAtPosition]);

  const handleTextDoubleClick = useCallback((event) => {
    if (tool !== 'text') return;
    
    event.preventDefault();
    event.stopPropagation();
    
    const coordinates = getCanvasCoordinates(event);
    const existingText = findTextAtPosition(coordinates.x, coordinates.y);
    
    if (existingText) {
      setTextInput({ 
        x: existingText.position.x, 
        y: existingText.position.y, 
        value: existingText.text,
        id: existingText.id,
        isEditing: true
      });
      setEditingTextId(existingText.id);
      setIsEditingText(true);
      setSelectedTextId(null);
    }
  }, [tool, getCanvasCoordinates, findTextAtPosition]);

  const handleTextComplete = useCallback((text) => {
    if (!textInput) {
      setTextInput(null);
      setIsEditingText(false);
      setEditingTextId(null);
      return;
    }

    if (!text.trim()) {
      setTextInput(null);
      setIsEditingText(false);
      setEditingTextId(null);
      return;
    }

    const context = contextRef.current;
    if (!context) return;

    const textItem = {
      id: textInput.id,
      text: text,
      position: { x: textInput.x, y: textInput.y },
      fontSize: textFormat.fontSize,
      fontFamily: textFormat.fontFamily,
      bold: textFormat.bold,
      italic: textFormat.italic,
      underline: textFormat.underline,
      strikethrough: textFormat.strikethrough,
      color: color
    };

    // ✅ Create the drawing data structure
    const textDrawingData = {
      drawing_id: textInput.id || `text_${Date.now()}`,
      tool_type: 'text',
      text_content: text,
      stroke_color: color,
      fontSize: textFormat.fontSize,
      fontFamily: textFormat.fontFamily,
      bold: textFormat.bold,
      italic: textFormat.italic,
      underline: textFormat.underline,
      strikethrough: textFormat.strikethrough,
      opacity: 1.0,
      drawing_data: {
        type: 'text',
        text: text,
        position: { x: textInput.x, y: textInput.y },
        fontSize: textFormat.fontSize,
        fontFamily: textFormat.fontFamily,
        bold: textFormat.bold,
        italic: textFormat.italic,
        underline: textFormat.underline,
        strikethrough: textFormat.strikethrough,
      }
    };

    // ✅ STEP 1: Update allDrawingsRef FIRST (before redrawing)
    if (textInput.isEditing) {
      // Find and REMOVE the old text from allDrawingsRef
      const existingIndex = allDrawingsRef.current.findIndex(item => {
        if (item.drawing_id === textInput.id) return true;
        if (item.tool_type === 'text' || item.drawing_data?.type === 'text') {
          const pos = item.drawing_data?.position || { x: item.x, y: item.y };
          // Check if position matches (with small tolerance for floating point)
          return Math.abs(pos.x - textInput.x) < 5 && Math.abs(pos.y - textInput.y) < 5;
        }
        return false;
      });
      
      if (existingIndex >= 0) {
        // ✅ Replace the old text with new text data
        console.log('✅ Replacing old text at index:', existingIndex);
        allDrawingsRef.current[existingIndex] = textDrawingData;
      } else {
        // If not found, just add as new
        console.log('⚠️ Old text not found, adding as new');
        allDrawingsRef.current.push(textDrawingData);
      }
    } else {
      // ✅ New text - add to allDrawingsRef
      allDrawingsRef.current.push(textDrawingData);
    }

    // ✅ STEP 2: Update existingTextItems
    setExistingTextItems(prev => {
      if (textInput.isEditing && editingTextId) {
        // Remove old and add updated
        const filtered = prev.filter(item => item.id !== editingTextId);
        return [...filtered, textItem];
      }
      return [...prev, textItem];
    });

    // ✅ STEP 3: Now redraw the ENTIRE canvas from allDrawingsRef
    // This will draw everything including the updated text
    const canvas = canvasRef.current;
    if (canvas) {
      // Clear the entire canvas
      context.clearRect(0, 0, canvas.width, canvas.height);
      context.fillStyle = highContrast ? '#000000' : '#ffffff';
      context.fillRect(0, 0, canvas.width, canvas.height);
      
      // Redraw ALL items from allDrawingsRef (which now has updated text)
      allDrawingsRef.current.forEach((item) => {
        try {
          const drawingData = item.drawing_data || {};
          
          context.save();
          context.strokeStyle = item.stroke_color || '#000000';
          context.lineWidth = item.stroke_width || 2;
          context.lineCap = 'round';
          context.lineJoin = 'round';
          context.globalAlpha = item.opacity || 1.0;
          
          if (item.tool_type === 'eraser') {
            context.globalCompositeOperation = 'destination-out';
          } else {
            context.globalCompositeOperation = 'source-over';
          }

          if (drawingData.type === 'path' && Array.isArray(drawingData.points) && drawingData.points.length > 0) {
            context.beginPath();
            context.moveTo(drawingData.points[0].x, drawingData.points[0].y);
            for (let i = 1; i < drawingData.points.length; i++) {
              context.lineTo(drawingData.points[i].x, drawingData.points[i].y);
            }
            context.stroke();
          } else if (drawingData.type === 'shape' && drawingData.start && drawingData.end) {
            drawShape(context, item.tool_type, drawingData.start, drawingData.end, item.stroke_color, item.stroke_width);
          } else if (drawingData.type === 'text' || item.tool_type === 'text') {
            // Draw text
            const textContent = item.text_content || drawingData.text || '';
            const position = drawingData.position || { x: item.x, y: item.y };
            const fontSize = drawingData.fontSize || item.fontSize || 20;
            const fontFamily = drawingData.fontFamily || item.fontFamily || 'Arial';
            const bold = drawingData.bold || item.bold || false;
            const italic = drawingData.italic || item.italic || false;
            const underline = drawingData.underline || item.underline || false;
            const strikethrough = drawingData.strikethrough || item.strikethrough || false;
            
            const fontStyle = `${italic ? 'italic ' : ''}${bold ? 'bold ' : ''}${fontSize}px ${fontFamily}`.trim();
            context.font = fontStyle;
            context.fillStyle = item.stroke_color || '#000000';
            context.textBaseline = 'top';
            
            const lines = textContent.split('\n');
            const lineHeight = fontSize * 1.2;
            
            lines.forEach((line, index) => {
              const yPosition = position.y + (index * lineHeight);
              context.fillText(line, position.x, yPosition);
              
              const textWidth = context.measureText(line).width;
              
              if (underline) {
                const lineY = yPosition + fontSize + 2;
                context.beginPath();
                context.moveTo(position.x, lineY);
                context.lineTo(position.x + textWidth, lineY);
                context.lineWidth = Math.max(1, fontSize / 16);
                context.strokeStyle = item.stroke_color || '#000000';
                context.stroke();
              }
              
              if (strikethrough) {
                const lineY = yPosition + (fontSize / 2);
                context.beginPath();
                context.moveTo(position.x, lineY);
                context.lineTo(position.x + textWidth, lineY);
                context.lineWidth = Math.max(1, fontSize / 16);
                context.strokeStyle = item.stroke_color || '#000000';
                context.stroke();
              }
            });
          }
          
          context.restore();
        } catch (error) {
          console.error('Error redrawing item:', error);
        }
      });
    }

    // ✅ STEP 4: Broadcast to backend
    if (onDrawing) {
      onDrawing({
        type: textInput.isEditing ? 'text_update' : 'text',
        drawing_id: textDrawingData.drawing_id,
        text: text,
        position: { x: textInput.x, y: textInput.y },
        font: `${textFormat.fontSize}px ${textFormat.fontFamily}`,
        color: color,
        underline: textFormat.underline,
        strikethrough: textFormat.strikethrough,
        bold: textFormat.bold,
        italic: textFormat.italic,
        fontSize: textFormat.fontSize,
        fontFamily: textFormat.fontFamily,
        meetingId,
        isEdit: textInput.isEditing,
      });
    }

    setTextInput(null);
    setIsEditingText(false);
    setEditingTextId(null);
  }, [textInput, color, textFormat, editingTextId, onDrawing, meetingId, highContrast]);

  const handleShapeTextComplete = useCallback((text) => {
    if (!shapeTextInput || !text.trim()) {
      setShapeTextInput(null);
      setIsEditingShapeText(false);
      return;
    }

    const context = contextRef.current;
    if (!context) return;

    setDrawnShapes(prev => prev.map(shape => 
      shape.id === shapeTextInput.shapeId 
        ? { ...shape, text: text }
        : shape
    ));

    drawText(text, { x: shapeTextInput.x, y: shapeTextInput.y }, true);

    if (onDrawing) {
      onDrawing({
        type: 'shape_text',
        shapeId: shapeTextInput.shapeId,
        text: text,
        position: { x: shapeTextInput.x, y: shapeTextInput.y },
        font: `${textFormat.fontSize}px ${textFormat.fontFamily}`,
        color: color,
        meetingId,
      });
    }

    setShapeTextInput(null);
    setIsEditingShapeText(false);
  }, [shapeTextInput, drawText, color, textFormat, onDrawing, meetingId]);

  const drawShape = (context, shapeType, start, end, strokeColor, lineWidth) => {
    context.strokeStyle = strokeColor;
    context.lineWidth = lineWidth;
    context.beginPath();

    switch (shapeType) {
      case 'line':
        context.moveTo(start.x, start.y);
        context.lineTo(end.x, end.y);
        break;
        
      case 'rectangle':
        const width = end.x - start.x;
        const height = end.y - start.y;
        context.rect(start.x, start.y, width, height);
        break;

      case 'square':
        const size = Math.min(
          Math.abs(end.x - start.x),
          Math.abs(end.y - start.y)
        );
        const squareEndX = start.x + (end.x > start.x ? size : -size);
        const squareEndY = start.y + (end.y > start.y ? size : -size);
        context.rect(start.x, start.y, squareEndX - start.x, squareEndY - start.y);
        break;
        
      case 'circle':
        const radius = Math.sqrt(
          Math.pow(end.x - start.x, 2) + 
          Math.pow(end.y - start.y, 2)
        );
        context.arc(start.x, start.y, radius, 0, 2 * Math.PI);
        break;

      case 'ellipse':
        const centerX = (start.x + end.x) / 2;
        const centerY = (start.y + end.y) / 2;
        const radiusX = Math.abs(end.x - start.x) / 2;
        const radiusY = Math.abs(end.y - start.y) / 2;
        context.ellipse(centerX, centerY, radiusX, radiusY, 0, 0, 2 * Math.PI);
        break;

      case 'triangle':
        const triWidth = end.x - start.x;
        const triHeight = end.y - start.y;
        context.moveTo(start.x + triWidth / 2, start.y);
        context.lineTo(start.x, end.y);
        context.lineTo(end.x, end.y);
        context.closePath();
        break;

      case 'arrow':
        context.moveTo(start.x, start.y);
        context.lineTo(end.x, end.y);
        
        const angle = Math.atan2(end.y - start.y, end.x - start.x);
        const headLength = 20;
        const headAngle = Math.PI / 6;
        
        context.moveTo(end.x, end.y);
        context.lineTo(
          end.x - headLength * Math.cos(angle - headAngle),
          end.y - headLength * Math.sin(angle - headAngle)
        );
        context.moveTo(end.x, end.y);
        context.lineTo(
          end.x - headLength * Math.cos(angle + headAngle),
          end.y - headLength * Math.sin(angle + headAngle)
        );
        break;
    }
    
    context.stroke();
  };

  const clearCanvas = useCallback(() => {
    const context = contextRef.current;
    const canvas = canvasRef.current;
    if (!context || !canvas) return;

    context.clearRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = highContrast ? '#000000' : '#ffffff';
    context.fillRect(0, 0, canvas.width, canvas.height);
    
    drawingStateRef.current.allPaths = [];
    
    setExistingTextItems([]);
    setDrawnShapes([]);
    allDrawingsRef.current = [];
    setSelectedItems([]);
    
    if (onDrawing) {
      onDrawing({ type: 'clear', meetingId });
    }
  }, [onDrawing, meetingId, highContrast]);

  const exportCanvas = useCallback((format) => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    if (format === 'png') {
      const link = document.createElement('a');
      link.download = `whiteboard-${meetingId}-${Date.now()}.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    }
  }, [meetingId]);

  const resizeCanvas = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || drawingStateRef.current.isDrawing) return;

    const rect = canvas.parentElement?.getBoundingClientRect();
    if (!rect || rect.width === 0 || rect.height === 0) {
      console.warn('Canvas parent has zero dimensions, skipping resize');
      return;
    }

    const dpr = window.devicePixelRatio || 1;
    
    let tempCanvas = null;
    let tempContext = null;
    
    if (canvas.width > 0 && canvas.height > 0 && canvasInitialized) {
      try {
        tempCanvas = document.createElement('canvas');
        tempContext = tempCanvas.getContext('2d');
        tempCanvas.width = canvas.width;
        tempCanvas.height = canvas.height;
        tempContext.drawImage(canvas, 0, 0);
      } catch (error) {
        console.warn('Could not save canvas content during resize:', error);
        tempCanvas = null;
      }
    }
    
    const newWidth = Math.max(rect.width, 100);
    const newHeight = Math.max(rect.height, 100);
    
    canvas.width = newWidth * dpr;
    canvas.height = newHeight * dpr;
    canvas.style.width = newWidth + 'px';
    canvas.style.height = newHeight + 'px';
    
    const context = canvas.getContext('2d');
    context.scale(dpr, dpr);
    context.lineCap = 'round';
    context.lineJoin = 'round';
    context.imageSmoothingEnabled = true;
    contextRef.current = context;

    if (tempCanvas && tempCanvas.width > 0 && tempCanvas.height > 0) {
      try {
        context.drawImage(tempCanvas, 0, 0, newWidth, newHeight);
      } catch (error) {
        console.warn('Could not restore canvas content after resize:', error);
      }
    }

    setCanvasInitialized(true);
  }, [canvasInitialized]);

  // Keyboard handler for delete
  useEffect(() => {
    const handleKeyDown = (e) => {
      // ✅ Don't handle keys when editing text
      if (isEditingText || isEditingShapeText) {
        return;
      }
      
      // ✅ Don't handle if target is an input or textarea
      if (e.target.tagName === 'INPUT' || 
          e.target.tagName === 'TEXTAREA' || 
          e.target.isContentEditable) {
        return;
      }
      
      if (tool === 'select' && selectedItems.length > 0) {
        if (e.key === 'Delete' || e.key === 'Backspace') {
          e.preventDefault();
          deleteSelectedItems();
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [tool, selectedItems, deleteSelectedItems, isEditingText, isEditingShapeText]);

  useImperativeHandle(ref, () => ({
    getCanvas: () => canvasRef.current,
    getContext: () => contextRef.current,
    clear: clearCanvas,
    getImageData: () => canvasRef.current?.toDataURL(),
    exportPNG: () => exportCanvas('png'),
    exportSVG: () => exportCanvas('svg'),
    getAllPaths: () => drawingStateRef.current.allPaths,
    redrawFromData: redrawFromData,
    deleteSelected: deleteSelectedItems,
  }), [clearCanvas, exportCanvas, redrawFromData, deleteSelectedItems]);

  useEffect(() => {
    const timer = setTimeout(() => {
      resizeCanvas();
    }, 100);

    const resizeObserver = new ResizeObserver(() => {
      if (!drawingStateRef.current.isDrawing && canvasRef.current?.parentElement) {
        setTimeout(resizeCanvas, 50);
      }
    });

    if (canvasRef.current?.parentElement) {
      resizeObserver.observe(canvasRef.current.parentElement);
    }

    return () => {
      clearTimeout(timer);
      resizeObserver.disconnect();
    };
  }, [resizeCanvas]);

  useEffect(() => {
    if (!drawingStateRef.current.isDrawing && canvasInitialized) {
      setTimeout(resizeCanvas, 100);
    }
  }, [isFullscreen, resizeCanvas, canvasInitialized]);

  useEffect(() => {
    if (onRemoteDrawing) {
      onRemoteDrawing((drawingData) => {
        const context = contextRef.current;
        if (!context) return;

        const originalOp = context.globalCompositeOperation;
        const originalAlpha = context.globalAlpha;

        context.strokeStyle = drawingData.tool === 'eraser' ? '#FFFFFF' : drawingData.color;
        context.lineWidth = drawingData.tool === 'eraser' ? drawingData.brushSize * 2 : drawingData.brushSize;
        context.globalCompositeOperation = drawingData.tool === 'eraser' ? 'destination-out' : 'source-over';
        context.globalAlpha = drawingData.tool === 'highlighter' ? 0.3 : 1.0;

        if (drawingData.type === 'draw') {
          context.beginPath();
          context.moveTo(drawingData.from.x, drawingData.from.y);
          context.lineTo(drawingData.to.x, drawingData.to.y);
          context.stroke();
        }

        if (drawingData.type === 'text' && drawingData.text && drawingData.position) {
          const textItem = {
            id: drawingData.id || `text_${Date.now()}`,
            text: drawingData.text,
            position: drawingData.position,
            fontSize: drawingData.fontSize || 20,
            fontFamily: drawingData.fontFamily || 'Arial',
            bold: drawingData.bold || false,
            italic: drawingData.italic || false,
            underline: drawingData.underline || false,
            strikethrough: drawingData.strikethrough || false,
            color: drawingData.color || '#000000'
          };
          
          setExistingTextItems(prev => {
            const exists = prev.find(item => item.id === textItem.id);
            if (exists) {
              return prev.map(item => item.id === textItem.id ? textItem : item);
            }
            return [...prev, textItem];
          });
          
          drawText(drawingData.text, drawingData.position);
        }

        context.globalCompositeOperation = originalOp;
        context.globalAlpha = originalAlpha;
      });
    }
  }, [onRemoteDrawing, drawText]);

  return (
    <Box 
      sx={{ 
        width: '100%', 
        height: '100%', 
        position: 'relative',
        backgroundColor: highContrast ? '#000000' : 'white',
        minWidth: '100px',
        minHeight: '100px',
      }}
    >
      <canvas
        ref={canvasRef}
        onMouseDown={startDrawing}
        onMouseMove={draw}
        onMouseUp={stopDrawing}
        onMouseLeave={stopDrawing}
        onDoubleClick={tool === 'text' ? handleTextDoubleClick : handleCanvasDoubleClick}
        onTouchStart={startDrawing}
        onTouchMove={draw}
        onTouchEnd={stopDrawing}
        style={{
          width: '100%',
          height: '100%',
          display: 'block',
          backgroundColor: highContrast ? '#000000' : 'white',
          touchAction: 'none',
          cursor: tool === 'text' ? 'text' : tool === 'select' ? 'default' : (isDrawing ? 'none' : 'crosshair'),
        }}
      />

      {selectedTextId && tool === 'text' && (() => {
        const selectedItem = existingTextItems.find(item => item.id === selectedTextId);
        if (!selectedItem) return null;
        
        const context = contextRef.current;
        if (!context) return null;
        
        context.font = `${selectedItem.italic ? 'italic ' : ''}${selectedItem.bold ? 'bold ' : ''}${selectedItem.fontSize || 20}px ${selectedItem.fontFamily || 'Arial'}`;
        const lines = selectedItem.text.split('\n');
        const lineHeight = (selectedItem.fontSize || 20) * 1.2;
        
        let maxWidth = 0;
        lines.forEach(line => {
          const width = context.measureText(line).width;
          if (width > maxWidth) maxWidth = width;
        });
        
        const textHeight = lines.length * lineHeight;
        const padding = 4;
        
        const canvas = canvasRef.current;
        const scaleX = canvas.parentElement.clientWidth / canvas.width;
        const scaleY = canvas.parentElement.clientHeight / canvas.height;
        
        return (
          <Box
            sx={{
              position: 'absolute',
              left: `${(selectedItem.position.x - padding) * scaleX}px`,
              top: `${(selectedItem.position.y - padding) * scaleY}px`,
              width: `${(maxWidth + padding * 2) * scaleX}px`,
              height: `${(textHeight + padding * 2) * scaleY}px`,
              border: '2px solid #1976d2',
              backgroundColor: 'transparent',
              pointerEvents: 'none',
              zIndex: 998,
            }}
          >
            {[
              { top: -4, left: -4 },
              { top: -4, right: -4 },
              { bottom: -4, left: -4 },
              { bottom: -4, right: -4 },
              { top: '50%', left: -4, transform: 'translateY(-50%)' },
              { top: '50%', right: -4, transform: 'translateY(-50%)' },
              { left: '50%', top: -4, transform: 'translateX(-50%)' },
              { left: '50%', bottom: -4, transform: 'translateX(-50%)' },
            ].map((pos, idx) => (
              <Box
                key={idx}
                sx={{
                  position: 'absolute',
                  width: '8px',
                  height: '8px',
                  backgroundColor: '#1976d2',
                  border: '1px solid white',
                  borderRadius: '50%',
                  ...pos,
                }}
              />
            ))}
          </Box>
        );
      })()}

      {isEditingText && textInput && (() => {
        const canvas = canvasRef.current;
        if (!canvas) return null;
        
        const rect = canvas.getBoundingClientRect();
        const scaleX = rect.width / canvas.width;
        const scaleY = rect.height / canvas.height;
        
        const screenX = textInput.x * scaleX;
        const screenY = textInput.y * scaleY;
        
        return (
          <Box
            sx={{
              position: 'absolute',
              left: `${screenX}px`,
              top: `${screenY}px`,
              zIndex: 1000,
              minWidth: '200px',
              transform: 'translate(0, 0)',
            }}
          >
            <textarea
              autoFocus
              value={textInput.value || ''}
              onChange={(e) => setTextInput(prev => ({ ...prev, value: e.target.value }))}
              onBlur={(e) => {
                if (e.relatedTarget?.tagName !== 'BUTTON') {
                  handleTextComplete(e.target.value);
                }
              }}
              onKeyDown={(e) => {
                // ✅ CRITICAL: Stop propagation for all standard text editing shortcuts
                // This prevents whiteboard keyboard handlers from intercepting
                if (e.ctrlKey || e.metaKey) {
                  // Allow Ctrl+A, Ctrl+C, Ctrl+V, Ctrl+X, Ctrl+Z, Ctrl+Y
                  const allowedKeys = ['a', 'c', 'v', 'x', 'z', 'y'];
                  if (allowedKeys.includes(e.key.toLowerCase())) {
                    e.stopPropagation(); // ✅ Stop event from bubbling up
                    return; // Let the browser handle it normally
                  }
                }
                
                // Handle Enter for new line
                if (e.key === 'Enter' && !e.shiftKey && !e.ctrlKey) {
                  const textarea = e.target;
                  const start = textarea.selectionStart;
                  const end = textarea.selectionEnd;
                  const value = textarea.value;
                  const newValue = value.substring(0, start) + '\n' + value.substring(end);
                  setTextInput(prev => ({ ...prev, value: newValue }));
                  setTimeout(() => {
                    textarea.selectionStart = textarea.selectionEnd = start + 1;
                  }, 0);
                  e.preventDefault();
                  e.stopPropagation();
                } else if (e.key === 'Escape') {
                  setTextInput(null);
                  setIsEditingText(false);
                  e.stopPropagation();
                } else if (e.key === 'Enter' && (e.shiftKey || e.ctrlKey)) {
                  handleTextComplete(e.target.value);
                  e.stopPropagation();
                }
                
                // ✅ Stop propagation for all other keys to prevent interference
                e.stopPropagation();
              }}
              // ✅ Also stop propagation on keyup to be safe
              onKeyUp={(e) => e.stopPropagation()}
              style={{
                fontSize: `${textFormat?.fontSize || 20}px`,
                color: color,
                border: '2px dashed #1976d2',
                background: 'rgba(255, 255, 255, 0.95)',
                padding: '8px 12px',
                minWidth: '200px',
                minHeight: '60px',
                maxWidth: '500px',
                fontFamily: textFormat?.fontFamily || 'Arial',
                fontWeight: textFormat?.bold ? 'bold' : 'normal',
                fontStyle: textFormat?.italic ? 'italic' : 'normal',
                textDecoration: (() => {
                  const decorations = [];
                  if (textFormat?.underline) decorations.push('underline');
                  if (textFormat?.strikethrough) decorations.push('line-through');
                  return decorations.length > 0 ? decorations.join(' ') : 'none';
                })(),
                outline: 'none',
                borderRadius: '4px',
                resize: 'both',
                overflow: 'auto',
                whiteSpace: 'pre-wrap',
                wordWrap: 'break-word',
              }}
              placeholder="Type text here... (Shift+Enter to finish, Esc to cancel)"
            />
            
            <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
              <button
                onClick={() => handleTextComplete(textInput.value)}
                style={{
                  padding: '4px 12px',
                  backgroundColor: '#1976d2',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '12px',
                }}
              >
                Finish (Shift+Enter)
              </button>
              <button
                onClick={() => {
                  setTextInput(null);
                  setIsEditingText(false);
                }}
                style={{
                  padding: '4px 12px',
                  backgroundColor: '#dc3545',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '12px',
                }}
              >
                Cancel (Esc)
              </button>
            </Box>
          </Box>
        );
      })()}

      {isEditingShapeText && shapeTextInput && (
        <Box
          sx={{
            position: 'absolute',
            left: shapeTextInput.x,
            top: shapeTextInput.y,
            zIndex: 1000,
            minWidth: '150px',
          }}
        >
          <textarea
            autoFocus
            value={shapeTextInput.value || ''}
            onChange={(e) => setShapeTextInput(prev => ({ ...prev, value: e.target.value }))}
            onBlur={(e) => {
              if (e.relatedTarget?.tagName !== 'BUTTON') {
                handleShapeTextComplete(e.target.value);
              }
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                const textarea = e.target;
                const start = textarea.selectionStart;
                const end = textarea.selectionEnd;
                const value = textarea.value;
                const newValue = value.substring(0, start) + '\n' + value.substring(end);
                setShapeTextInput(prev => ({ ...prev, value: newValue }));
                setTimeout(() => {
                  textarea.selectionStart = textarea.selectionEnd = start + 1;
                }, 0);
                e.preventDefault();
              } else if (e.key === 'Escape') {
                setShapeTextInput(null);
                setIsEditingShapeText(false);
              } else if (e.key === 'Enter' && e.shiftKey) {
                handleShapeTextComplete(e.target.value);
              }
            }}
            style={{
              fontSize: `${textFormat?.fontSize || 16}px`,
              color: color,
              border: '2px solid #1976d2',
              background: 'rgba(255, 255, 255, 0.95)',
              padding: '4px 8px',
              minWidth: '150px',
              minHeight: '40px',
              maxWidth: '300px',
              fontFamily: textFormat?.fontFamily || 'Arial',
              fontWeight: textFormat?.bold ? 'bold' : 'normal',
              fontStyle: textFormat?.italic ? 'italic' : 'normal',
              outline: 'none',
              borderRadius: '4px',
              resize: 'both',
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
            }}
            placeholder="Type text in shape..."
          />
          
          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
            <button
              onClick={() => handleShapeTextComplete(shapeTextInput.value)}
              style={{
                padding: '3px 10px',
                backgroundColor: '#1976d2',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '11px',
              }}
            >
              Done
            </button>
            <button
              onClick={() => {
                setShapeTextInput(null);
                setIsEditingShapeText(false);
              }}
              style={{
                padding: '3px 10px',
                backgroundColor: '#dc3545',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '11px',
              }}
            >
              Cancel
            </button>
          </Box>
        </Box>
      )}

      {isDrawing && tool !== 'text' && tool !== 'select' && (
        <>
          <Box
            sx={{
              position: 'absolute',
              top: 10,
              left: 10,
              backgroundColor: 'rgba(25, 118, 210, 0.8)',
              color: 'white',
              padding: '2px 6px',
              borderRadius: 1,
              fontSize: '0.7rem',
              pointerEvents: 'none',
              zIndex: 10,
            }}
          >
            {tool} - {color}
          </Box>
          
          {previewShape && (
            <Box
              sx={{
                position: 'absolute',
                bottom: 10,
                right: 10,
                backgroundColor: 'rgba(0, 0, 0, 0.7)',
                color: 'white',
                padding: '4px 8px',
                borderRadius: 1,
                fontSize: '0.7rem',
                pointerEvents: 'none',
                zIndex: 10,
              }}
            >
              Width: {Math.abs(previewShape.end.x - previewShape.start.x).toFixed(0)}px | 
              Height: {Math.abs(previewShape.end.y - previewShape.start.y).toFixed(0)}px
            </Box>
          )}
        </>
      )}

      {tool === 'select' && selectedItems.length > 0 && (
        <Box
          sx={{
            position: 'absolute',
            top: 10,
            left: 10,
            backgroundColor: 'rgba(25, 118, 210, 0.9)',
            color: 'white',
            padding: '6px 12px',
            borderRadius: 1,
            fontSize: '0.75rem',
            pointerEvents: 'none',
            zIndex: 10,
            display: 'flex',
            alignItems: 'center',
            gap: 1,
          }}
        >
          <span>{selectedItems.length} item{selectedItems.length > 1 ? 's' : ''} selected</span>
          <span style={{ fontSize: '0.65rem', opacity: 0.8 }}>
            (Press Delete to remove)
          </span>
        </Box>
      )}
    </Box>
  );
});

WhiteboardCanvas.displayName = 'WhiteboardCanvas';

export default WhiteboardCanvas;