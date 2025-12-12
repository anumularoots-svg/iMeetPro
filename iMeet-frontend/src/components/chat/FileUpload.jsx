// src/components/chat/FileUpload.jsx - FIXED WITH WEBSOCKET
import React, { useState, useRef, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  LinearProgress,
  Paper,
  Alert,
  Chip,
  Avatar,
  Fade,
  useTheme
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  AttachFile as FileIcon,
  Image as ImageIcon,
  PictureAsPdf as PdfIcon,
  Description as DocIcon,
  Delete as DeleteIcon,
  Check as CheckIcon,
  Error as ErrorIcon,
  Close as CloseIcon
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

const DropZone = styled(Paper)(({ theme, isDragActive, hasError }) => ({
  border: `2px dashed ${
    hasError 
      ? theme.palette.error.main 
      : isDragActive 
        ? theme.palette.primary.main 
        : theme.palette.divider
  }`,
  borderRadius: theme.spacing(2),
  padding: theme.spacing(2),
  textAlign: 'center',
  cursor: 'pointer',
  background: isDragActive 
    ? theme.palette.primary.light + '22'
    : hasError
      ? theme.palette.error.light + '22'
      : theme.palette.background.paper,
  transition: 'all 0.3s ease',
  '&:hover': {
    borderColor: theme.palette.primary.main,
    background: theme.palette.primary.light + '11',
  }
}));

const FilePreview = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(1),
  background: theme.palette.grey[50],
  borderRadius: theme.spacing(1),
  border: `1px solid ${theme.palette.divider}`,
  marginTop: theme.spacing(1)
}));

const FileInfo = styled(Box)(({ theme }) => ({
  flex: 1,
  minWidth: 0
}));

const FileUpload = ({
  onFileUpload,
  onCancel,
  maxSize = 10 * 1024 * 1024, // 10MB default
  allowedTypes = ['image/*', 'application/pdf', '.doc,.docx,.txt'],
  multiple = false
}) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isUploading, setIsUploading] = useState(false);
  const [isDragActive, setIsDragActive] = useState(false);
  const [error, setError] = useState(null);
  const fileInputRef = useRef(null);
  const theme = useTheme();

  const getFileIcon = (file) => {
    const type = file.type;
    const name = file.name.toLowerCase();
    
    if (type.startsWith('image/')) {
      return <ImageIcon color="primary" />;
    } else if (type === 'application/pdf' || name.endsWith('.pdf')) {
      return <PdfIcon color="error" />;
    } else if (name.endsWith('.doc') || name.endsWith('.docx')) {
      return <DocIcon color="info" />;
    } else {
      return <FileIcon color="action" />;
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const validateFile = (file) => {
    // Check file size
    if (file.size > maxSize) {
      return `File size exceeds ${formatFileSize(maxSize)} limit`;
    }

    // Check file type
    const isAllowed = allowedTypes.some(type => {
      if (type.includes('*')) {
        const baseType = type.split('/')[0];
        return file.type.startsWith(baseType + '/');
      } else if (type.startsWith('.')) {
        return file.name.toLowerCase().endsWith(type.toLowerCase());
      } else {
        return file.type === type;
      }
    });

    if (!isAllowed) {
      return `File type not allowed. Allowed types: ${allowedTypes.join(', ')}`;
    }

    return null;
  };

  const handleFileSelect = (files) => {
    if (files.length === 0) return;
    
    const file = files[0];
    const error = validateFile(file);
    
    if (error) {
      setError(error);
      setSelectedFile(null);
    } else {
      setError(null);
      setSelectedFile(file);
    }
  };

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(false);
    const files = e.dataTransfer.files;
    handleFileSelect(files);
  }, []);

  const handleDragOver = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(true);
  }, []);

  const handleDragLeave = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(false);
  }, []);

  const handleFileInputChange = (e) => {
    if (e.target.files) {
      handleFileSelect(e.target.files);
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    setUploadProgress(0);

    try {
      // Convert file to base64 for sending through WebSocket
      const reader = new FileReader();
      
      reader.onloadend = () => {
        const base64 = reader.result;
        
        // Simulate upload progress
        const progressInterval = setInterval(() => {
          setUploadProgress(prev => {
            if (prev >= 100) {
              clearInterval(progressInterval);
              return 100;
            }
            return prev + 10;
          });
        }, 100);

        // Send file data to parent
        setTimeout(() => {
          clearInterval(progressInterval);
          setUploadProgress(100);
          
          const fileData = {
            fileName: selectedFile.name,
            fileSize: selectedFile.size,
            fileType: selectedFile.type,
            fileData: base64,
            uploadedAt: new Date().toISOString()
          };
          
          onFileUpload(fileData);
          
          // Reset state
          setSelectedFile(null);
          setUploadProgress(0);
          setIsUploading(false);
          setError(null);
        }, 1000);
      };

      reader.onerror = () => {
        setError('Failed to read file');
        setIsUploading(false);
      };

      reader.readAsDataURL(selectedFile);
      
    } catch (error) {
      setError('Upload failed. Please try again.');
      setIsUploading(false);
    }
  };

  return (
    <Box>
      {/* Compact Drop Zone */}
      <DropZone
        isDragActive={isDragActive}
        hasError={!!error}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => fileInputRef.current?.click()}
        elevation={0}
      >
        <Box display="flex" alignItems="center" justifyContent="center" gap={1}>
          <UploadIcon color={error ? "error" : "primary"} />
          <Typography variant="body2" color={error ? "error" : "text.secondary"}>
            {selectedFile 
              ? `Selected: ${selectedFile.name}`
              : isDragActive 
                ? "Drop file here" 
                : "Click or drag file to upload"
            }
          </Typography>
        </Box>
        
        {!selectedFile && (
          <Typography variant="caption" color="text.secondary" display="block" mt={0.5}>
            Max size: {formatFileSize(maxSize)}
          </Typography>
        )}
      </DropZone>

      {/* Hidden File Input */}
      <input
        ref={fileInputRef}
        type="file"
        accept={allowedTypes.join(',')}
        onChange={handleFileInputChange}
        style={{ display: 'none' }}
      />

      {/* Error Display */}
      {error && (
        <Alert 
          severity="error" 
          sx={{ mt: 1, py: 0.5 }}
          onClose={() => setError(null)}
        >
          {error}
        </Alert>
      )}

      {/* Selected File Preview */}
      {selectedFile && !isUploading && (
        <FilePreview>
          {getFileIcon(selectedFile)}
          
          <FileInfo>
            <Typography variant="body2" noWrap>
              {selectedFile.name}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {formatFileSize(selectedFile.size)}
            </Typography>
          </FileInfo>

          <IconButton
            size="small"
            onClick={() => setSelectedFile(null)}
            color="error"
          >
            <DeleteIcon fontSize="small" />
          </IconButton>
        </FilePreview>
      )}

      {/* Upload Progress */}
      {isUploading && (
        <Box mt={1}>
          <Box display="flex" justifyContent="space-between" mb={0.5}>
            <Typography variant="caption">Uploading...</Typography>
            <Typography variant="caption">{uploadProgress}%</Typography>
          </Box>
          <LinearProgress
            variant="determinate"
            value={uploadProgress}
            sx={{ height: 4, borderRadius: 2 }}
          />
        </Box>
      )}

      {/* Action Buttons */}
      <Box display="flex" gap={1} mt={1.5} justifyContent="flex-end">
        <Button
          size="small"
          variant="outlined"
          onClick={onCancel}
          disabled={isUploading}
        >
          Cancel
        </Button>
        
        <Button
          size="small"
          variant="contained"
          onClick={handleUpload}
          disabled={!selectedFile || isUploading}
          startIcon={<UploadIcon />}
        >
          {isUploading ? 'Uploading...' : 'Send File'}
        </Button>
      </Box>
    </Box>
  );
};

export default FileUpload;