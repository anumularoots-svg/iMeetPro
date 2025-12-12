// components/common/FileUpload.jsx (Renamed and enhanced from ImageUpload)
import React, { useState, useRef } from 'react';
import {
  Box,
  Button,
  Avatar,
  IconButton,
  Typography,
  LinearProgress,
  Alert,
  Stack,
  useTheme,
  alpha,
  Fade,
  Backdrop,
  CircularProgress,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Chip
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  PhotoCamera as CameraIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  Close as CloseIcon,
  InsertDriveFile as FileIcon,
  TableChart as ExcelIcon,
  Description as DocumentIcon,
  Image as ImageIcon
} from '@mui/icons-material';

const FileUpload = ({
  value,
  onChange,
  onError,
  variant = "avatar", // "avatar" | "banner" | "square" | "document" | "excel"
  size = "large", // "small" | "medium" | "large"
  maxSize = 10 * 1024 * 1024, // 10MB for documents, 5MB for images
  acceptedFormats = ['image/jpeg', 'image/png', 'image/webp'],
  showProgress = true,
  disabled = false,
  placeholder = "Upload File",
  helperText = "",
  required = false,
  multiple = false,
  onFileAnalysis // Callback for file analysis (especially for Excel/CSV)
}) => {
  const theme = useTheme();
  const fileInputRef = useRef(null);
  const [preview, setPreview] = useState(value || null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [uploadedFiles, setUploadedFiles] = useState([]);

  // Set default formats based on variant
  const getDefaultFormats = () => {
    switch (variant) {
      case 'excel':
        return [
          'application/vnd.ms-excel',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          'text/csv'
        ];
      case 'document':
        return [
          'application/pdf',
          'application/msword',
          'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
          'text/plain',
          'application/vnd.ms-excel',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ];
      default:
        return ['image/jpeg', 'image/png', 'image/webp'];
    }
  };

  const formats = acceptedFormats.length > 0 ? acceptedFormats : getDefaultFormats();

  const getAvatarSize = () => {
    switch (size) {
      case 'small': return 64;
      case 'medium': return 96;
      case 'large': return 128;
      default: return 96;
    }
  };

  const getBannerHeight = () => {
    switch (size) {
      case 'small': return 120;
      case 'medium': return 160;
      case 'large': return 200;
      default: return 160;
    }
  };

  const validateFile = (file) => {
    if (!file) return "No file selected";
    
    const maxFileSize = variant === 'excel' || variant === 'document' ? 10 * 1024 * 1024 : maxSize;
    
    if (file.size > maxFileSize) {
      return `File size must be less than ${(maxFileSize / (1024 * 1024)).toFixed(1)}MB`;
    }
    
    if (!formats.includes(file.type)) {
      const formatNames = formats.map(f => f.split('/')[1]).join(', ');
      return `Please upload ${formatNames} files only`;
    }
    
    return null;
  };

  const getFileIcon = (fileType) => {
    if (fileType.includes('excel') || fileType.includes('spreadsheet') || fileType.includes('csv')) {
      return <ExcelIcon color="success" />;
    }
    if (fileType.includes('pdf') || fileType.includes('document') || fileType.includes('text')) {
      return <DocumentIcon color="primary" />;
    }
    if (fileType.includes('image')) {
      return <ImageIcon color="secondary" />;
    }
    return <FileIcon color="info" />;
  };

  const handleFileSelect = async (files) => {
    const fileList = Array.from(files);
    
    if (!multiple && fileList.length > 1) {
      setError('Only one file is allowed');
      if (onError) onError('Only one file is allowed');
      return;
    }

    setError('');
    setUploading(true);
    setProgress(0);

    try {
      const processedFiles = [];
      
      for (const file of fileList) {
        const validationError = validateFile(file);
        if (validationError) {
          setError(validationError);
          if (onError) onError(validationError);
          continue;
        }

        // For images, create preview
        if (file.type.startsWith('image/')) {
          const reader = new FileReader();
          reader.onload = (e) => {
            setPreview(e.target.result);
          };
          reader.readAsDataURL(file);
        }

        // For Excel/CSV files, perform analysis
        if (variant === 'excel' && onFileAnalysis) {
          try {
            await onFileAnalysis(file);
          } catch (analysisError) {
            console.error('File analysis failed:', analysisError);
          }
        }

        processedFiles.push({
          file,
          name: file.name,
          size: file.size,
          type: file.type,
          lastModified: file.lastModified
        });
      }

      // Simulate upload progress
      const interval = setInterval(() => {
        setProgress(prev => {
          if (prev >= 90) {
            clearInterval(interval);
            return 90;
          }
          return prev + 10;
        });
      }, 100);

      // Call onChange with file(s)
      if (onChange) {
        if (multiple) {
          await onChange(processedFiles.map(pf => pf.file));
          setUploadedFiles(processedFiles);
        } else {
          await onChange(processedFiles[0]?.file);
        }
      }

      setProgress(100);
      setTimeout(() => {
        setUploading(false);
        setProgress(0);
      }, 500);

    } catch (error) {
      setError(error.message || 'Upload failed');
      if (onError) onError(error.message);
      setUploading(false);
      setProgress(0);
    }
  };

  const handleInputChange = (event) => {
    const files = event.target.files;
    if (files && files.length > 0) {
      handleFileSelect(files);
    }
  };

  const handleDragEnter = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
      handleFileSelect(files);
    }
  };

  const handleRemove = () => {
    setPreview(null);
    setError('');
    setUploadedFiles([]);
    if (onChange) onChange(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const openFileDialog = () => {
    if (!disabled && fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const renderAvatarUpload = () => (
    <Box position="relative" display="inline-block">
      <Avatar
        src={preview}
        sx={{
          width: getAvatarSize(),
          height: getAvatarSize(),
          bgcolor: preview ? 'transparent' : alpha(theme.palette.primary.main, 0.1),
          border: dragActive ? `2px dashed ${theme.palette.primary.main}` : 'none',
          cursor: disabled ? 'default' : 'pointer',
          transition: 'all 0.3s ease',
          '&:hover': !disabled ? {
            transform: 'scale(1.05)',
            boxShadow: theme.shadows[4],
          } : {},
        }}
        onClick={openFileDialog}
        onDragEnter={handleDragEnter}
        onDragLeave={handleDragLeave}
        onDragOver={handleDragOver}
        onDrop={handleDrop}
      >
        {!preview && <CameraIcon sx={{ fontSize: getAvatarSize() * 0.4 }} />}
      </Avatar>

      {preview && !disabled && (
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            handleRemove();
          }}
          sx={{
            position: 'absolute',
            top: -8,
            right: -8,
            bgcolor: theme.palette.error.main,
            color: 'white',
            '&:hover': {
              bgcolor: theme.palette.error.dark,
            }
          }}
        >
          <CloseIcon fontSize="small" />
        </IconButton>
      )}
    </Box>
  );

  const renderDocumentUpload = () => (
    <Paper
      elevation={dragActive ? 8 : 2}
      sx={{
        width: '100%',
        minHeight: getBannerHeight(),
        border: dragActive 
          ? `2px dashed ${theme.palette.primary.main}`
          : `2px dashed ${alpha(theme.palette.divider, 0.5)}`,
        borderRadius: 3,
        position: 'relative',
        overflow: 'hidden',
        cursor: disabled ? 'default' : 'pointer',
        bgcolor: dragActive 
          ? alpha(theme.palette.primary.main, 0.05)
          : alpha(theme.palette.background.paper, 0.8),
        transition: 'all 0.3s ease',
        '&:hover': !disabled ? {
          borderColor: theme.palette.primary.main,
          bgcolor: alpha(theme.palette.primary.main, 0.02),
          transform: 'translateY(-2px)',
          boxShadow: theme.shadows[4]
        } : {},
      }}
      onClick={openFileDialog}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      <Stack
        alignItems="center"
        justifyContent="center"
        height="100%"
        spacing={2}
        sx={{ p: 3 }}
      >
        {variant === 'excel' ? (
          <ExcelIcon sx={{ fontSize: 64, color: theme.palette.success.main, opacity: 0.8 }} />
        ) : (
          <UploadIcon sx={{ fontSize: 64, color: theme.palette.primary.main, opacity: 0.8 }} />
        )}
        
        <Typography variant="h6" textAlign="center" color="text.primary">
          {dragActive ? 'Drop files here' : placeholder}
        </Typography>
        
        <Typography variant="body2" color="text.secondary" textAlign="center">
          {dragActive 
            ? 'Release to upload'
            : `Drag & drop ${multiple ? 'files' : 'a file'} here, or click to browse`
          }
        </Typography>
        
        <Typography variant="caption" color="text.disabled" textAlign="center">
          Supported: {formats.map(f => f.split('/')[1].toUpperCase()).join(', ')}
        </Typography>
        
        <Typography variant="caption" color="text.disabled">
          Max size: {Math.round(maxSize / (1024 * 1024))}MB
        </Typography>
      </Stack>
    </Paper>
  );

  const renderBannerUpload = () => (
    <Box
      sx={{
        width: '100%',
        height: getBannerHeight(),
        border: dragActive 
          ? `2px dashed ${theme.palette.primary.main}`
          : `2px dashed ${alpha(theme.palette.divider, 0.5)}`,
        borderRadius: 2,
        position: 'relative',
        overflow: 'hidden',
        cursor: disabled ? 'default' : 'pointer',
        bgcolor: preview ? 'transparent' : alpha(theme.palette.background.paper, 0.5),
        backgroundImage: preview ? `url(${preview})` : 'none',
        backgroundSize: 'cover',
        backgroundPosition: 'center',
        transition: 'all 0.3s ease',
        '&:hover': !disabled ? {
          borderColor: theme.palette.primary.main,
          bgcolor: alpha(theme.palette.primary.main, 0.02),
        } : {},
      }}
      onClick={openFileDialog}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {!preview && (
        <Stack
          alignItems="center"
          justifyContent="center"
          height="100%"
          spacing={1}
          sx={{ color: theme.palette.text.secondary }}
        >
          <UploadIcon sx={{ fontSize: 48, opacity: 0.7 }} />
          <Typography variant="body2" textAlign="center">
            {placeholder}
          </Typography>
          <Typography variant="caption" color="text.disabled">
            Drag & drop or click to upload
          </Typography>
        </Stack>
      )}

      {preview && !disabled && (
        <IconButton
          onClick={(e) => {
            e.stopPropagation();
            handleRemove();
          }}
          sx={{
            position: 'absolute',
            top: 8,
            right: 8,
            bgcolor: alpha(theme.palette.background.paper, 0.9),
            '&:hover': {
              bgcolor: theme.palette.background.paper,
            }
          }}
        >
          <DeleteIcon />
        </IconButton>
      )}
    </Box>
  );

  const renderUploadedFiles = () => (
    <Box sx={{ mt: 2 }}>
      <Typography variant="subtitle2" gutterBottom>
        Uploaded Files ({uploadedFiles.length})
      </Typography>
      <List dense>
        {uploadedFiles.map((fileInfo, index) => (
          <ListItem key={index} sx={{ bgcolor: alpha(theme.palette.success.main, 0.05), borderRadius: 1, mb: 1 }}>
            <ListItemIcon>
              {getFileIcon(fileInfo.type)}
            </ListItemIcon>
            <ListItemText
              primary={fileInfo.name}
              secondary={`${formatFileSize(fileInfo.size)} • ${fileInfo.type}`}
            />
            <Chip label="Uploaded" color="success" size="small" />
          </ListItem>
        ))}
      </List>
    </Box>
  );

  const getUploadComponent = () => {
    switch (variant) {
      case 'avatar':
        return renderAvatarUpload();
      case 'excel':
      case 'document':
        return renderDocumentUpload();
      default:
        return renderBannerUpload();
    }
  };

  return (
    <Box>
      <input
        ref={fileInputRef}
        type="file"
        accept={formats.join(',')}
        onChange={handleInputChange}
        style={{ display: 'none' }}
        disabled={disabled}
        multiple={multiple}
      />

      {getUploadComponent()}

      {/* Uploaded Files List */}
      {multiple && uploadedFiles.length > 0 && renderUploadedFiles()}

      {/* Progress Bar */}
      {showProgress && uploading && (
        <Box mt={2}>
          <LinearProgress
            variant="determinate"
            value={progress}
            sx={{
              borderRadius: 1,
              height: 6,
              bgcolor: alpha(theme.palette.primary.main, 0.1),
              '& .MuiLinearProgress-bar': {
                borderRadius: 1,
              }
            }}
          />
          <Typography variant="caption" color="text.secondary" mt={0.5} display="block">
            Uploading... {progress}%
          </Typography>
        </Box>
      )}

      {/* Helper Text */}
      {helperText && !error && (
        <Typography variant="caption" color="text.secondary" mt={1} display="block">
          {helperText}
        </Typography>
      )}

      {/* Error Message */}
      {error && (
        <Fade in={!!error}>
          <Alert 
            severity="error" 
            sx={{ mt: 1 }}
            onClose={() => setError('')}
          >
            {error}
          </Alert>
        </Fade>
      )}

      {/* Loading Backdrop */}
      <Backdrop
        sx={{ 
          color: '#fff', 
          zIndex: theme.zIndex.drawer + 1,
          bgcolor: alpha(theme.palette.background.default, 0.8)
        }}
        open={uploading}
      >
        <Stack alignItems="center" spacing={2}>
          <CircularProgress color="inherit" />
          <Typography variant="h6">
            {variant === 'excel' ? 'Processing Excel file...' : 'Uploading...'}
          </Typography>
        </Stack>
      </Backdrop>
    </Box>
  );
};

export default FileUpload;