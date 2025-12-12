import React from 'react';

// Shape drawing utilities for whiteboard
export class WhiteboardShapes {
  static drawLine(context, startPoint, endPoint, strokeStyle, lineWidth) {
    if (!context || !startPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    context.lineCap = 'round';
    
    context.beginPath();
    context.moveTo(startPoint.x, startPoint.y);
    context.lineTo(endPoint.x, endPoint.y);
    context.stroke();
    
    context.restore();
  }

  static drawRectangle(context, startPoint, endPoint, strokeStyle, lineWidth, filled = false, fillStyle = null) {
    if (!context || !startPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    
    const width = endPoint.x - startPoint.x;
    const height = endPoint.y - startPoint.y;
    
    context.beginPath();
    context.rect(startPoint.x, startPoint.y, width, height);
    
    if (filled && fillStyle) {
      context.fillStyle = fillStyle;
      context.fill();
    }
    
    context.stroke();
    context.restore();
  }

  static drawCircle(context, centerPoint, endPoint, strokeStyle, lineWidth, filled = false, fillStyle = null) {
    if (!context || !centerPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    
    const radius = Math.sqrt(
      Math.pow(endPoint.x - centerPoint.x, 2) + 
      Math.pow(endPoint.y - centerPoint.y, 2)
    );
    
    context.beginPath();
    context.arc(centerPoint.x, centerPoint.y, radius, 0, 2 * Math.PI);
    
    if (filled && fillStyle) {
      context.fillStyle = fillStyle;
      context.fill();
    }
    
    context.stroke();
    context.restore();
  }

  static drawEllipse(context, startPoint, endPoint, strokeStyle, lineWidth, filled = false, fillStyle = null) {
    if (!context || !startPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    
    const centerX = (startPoint.x + endPoint.x) / 2;
    const centerY = (startPoint.y + endPoint.y) / 2;
    const radiusX = Math.abs(endPoint.x - startPoint.x) / 2;
    const radiusY = Math.abs(endPoint.y - startPoint.y) / 2;
    
    context.beginPath();
    context.ellipse(centerX, centerY, radiusX, radiusY, 0, 0, 2 * Math.PI);
    
    if (filled && fillStyle) {
      context.fillStyle = fillStyle;
      context.fill();
    }
    
    context.stroke();
    context.restore();
  }

  static drawTriangle(context, startPoint, endPoint, strokeStyle, lineWidth, filled = false, fillStyle = null) {
    if (!context || !startPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    
    const width = endPoint.x - startPoint.x;
    const height = endPoint.y - startPoint.y;
    
    // Calculate triangle points
    const topPoint = { x: startPoint.x + width / 2, y: startPoint.y };
    const bottomLeft = { x: startPoint.x, y: endPoint.y };
    const bottomRight = { x: endPoint.x, y: endPoint.y };
    
    context.beginPath();
    context.moveTo(topPoint.x, topPoint.y);
    context.lineTo(bottomLeft.x, bottomLeft.y);
    context.lineTo(bottomRight.x, bottomRight.y);
    context.closePath();
    
    if (filled && fillStyle) {
      context.fillStyle = fillStyle;
      context.fill();
    }
    
    context.stroke();
    context.restore();
  }

  static drawArrow(context, startPoint, endPoint, strokeStyle, lineWidth) {
    if (!context || !startPoint || !endPoint) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    context.lineCap = 'round';
    context.lineJoin = 'round';
    
    // Draw main line
    context.beginPath();
    context.moveTo(startPoint.x, startPoint.y);
    context.lineTo(endPoint.x, endPoint.y);
    context.stroke();
    
    // Calculate arrow head
    const angle = Math.atan2(endPoint.y - startPoint.y, endPoint.x - startPoint.x);
    const headLength = 20;
    const headAngle = Math.PI / 6;
    
    // Draw arrow head
    context.beginPath();
    context.moveTo(endPoint.x, endPoint.y);
    context.lineTo(
      endPoint.x - headLength * Math.cos(angle - headAngle),
      endPoint.y - headLength * Math.sin(angle - headAngle)
    );
    context.moveTo(endPoint.x, endPoint.y);
    context.lineTo(
      endPoint.x - headLength * Math.cos(angle + headAngle),
      endPoint.y - headLength * Math.sin(angle + headAngle)
    );
    context.stroke();
    
    context.restore();
  }

  static drawFreehand(context, points, strokeStyle, lineWidth) {
    if (!context || !points || points.length < 2) return;
    
    context.save();
    context.strokeStyle = strokeStyle;
    context.lineWidth = lineWidth;
    context.lineCap = 'round';
    context.lineJoin = 'round';
    
    context.beginPath();
    context.moveTo(points[0].x, points[0].y);
    
    // Use quadratic curves for smoother lines
    for (let i = 1; i < points.length - 1; i++) {
      const currentPoint = points[i];
      const nextPoint = points[i + 1];
      const controlX = (currentPoint.x + nextPoint.x) / 2;
      const controlY = (currentPoint.y + nextPoint.y) / 2;
      
      context.quadraticCurveTo(currentPoint.x, currentPoint.y, controlX, controlY);
    }
    
    // Draw to the last point
    if (points.length > 1) {
      const lastPoint = points[points.length - 1];
      context.lineTo(lastPoint.x, lastPoint.y);
    }
    
    context.stroke();
    context.restore();
  }

  static drawText(context, text, position, font, fillStyle, strokeStyle = null, lineWidth = 1) {
    if (!context || !text || !position) return;
    
    context.save();
    context.font = font || '16px Arial';
    context.fillStyle = fillStyle || '#000000';
    context.textBaseline = 'top';
    
    if (strokeStyle) {
      context.strokeStyle = strokeStyle;
      context.lineWidth = lineWidth;
      context.strokeText(text, position.x, position.y);
    }
    
    context.fillText(text, position.x, position.y);
    context.restore();
  }

  static eraseArea(context, centerPoint, radius) {
    if (!context || !centerPoint || !radius) return;
    
    context.save();
    context.globalCompositeOperation = 'destination-out';
    
    context.beginPath();
    context.arc(centerPoint.x, centerPoint.y, radius, 0, 2 * Math.PI);
    context.fill();
    
    context.restore();
  }

  // Utility function to get shape bounds
  static getShapeBounds(shapeType, startPoint, endPoint) {
    if (!startPoint || !endPoint) return null;
    
    const minX = Math.min(startPoint.x, endPoint.x);
    const minY = Math.min(startPoint.y, endPoint.y);
    const maxX = Math.max(startPoint.x, endPoint.x);
    const maxY = Math.max(startPoint.y, endPoint.y);
    
    return {
      x: minX,
      y: minY,
      width: maxX - minX,
      height: maxY - minY
    };
  }

  // Check if a point is inside a shape
  static isPointInShape(point, shapeType, startPoint, endPoint) {
    if (!point || !startPoint || !endPoint) return false;
    
    switch (shapeType) {
      case 'rectangle':
        return point.x >= Math.min(startPoint.x, endPoint.x) &&
               point.x <= Math.max(startPoint.x, endPoint.x) &&
               point.y >= Math.min(startPoint.y, endPoint.y) &&
               point.y <= Math.max(startPoint.y, endPoint.y);
               
      case 'circle':
        const radius = Math.sqrt(
          Math.pow(endPoint.x - startPoint.x, 2) + 
          Math.pow(endPoint.y - startPoint.y, 2)
        );
        const distance = Math.sqrt(
          Math.pow(point.x - startPoint.x, 2) + 
          Math.pow(point.y - startPoint.y, 2)
        );
        return distance <= radius;
        
      default:
        return false;
    }
  }
}

export default WhiteboardShapes;