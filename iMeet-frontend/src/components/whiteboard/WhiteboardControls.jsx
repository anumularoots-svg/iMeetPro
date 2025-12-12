import React, { useState, useEffect, useCallback } from 'react'; 
import {
  Box,
  ButtonGroup,
  IconButton,
  Typography,
  Tooltip,
  Divider,
  Chip,
  Paper,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Alert,
  CircularProgress,
} from '@mui/material';
import {
  // Action Tools
  Undo,
  Redo,
  Clear,
  Save,
  
  // Export Tools
  Download,
  Image,
  GetApp,
  
  // Status Tools
  CheckCircle,
  Error,
  Warning,
  Refresh,
  CloudSync,
  
  // Navigation Tools
  NavigateBefore,
  NavigateNext,
  Timeline,
  
  // Utility
  Settings,
  Upload,
} from '@mui/icons-material';

const WhiteboardControls = ({
  // Basic undo/redo
  canUndo = false,
  canRedo = false,
  onUndo,
  onRedo,
  
  // Actions
  onClear,
  onSave,
  onExport,
  onExportPNG,
  onExportSVG,
  onImport,
  
  // View controls
  onFullscreen,
  isFullscreen = false,
  onToggleGrid,
  showGrid = false,
  onZoomIn,
  onZoomOut,
  onResetZoom,
  zoomLevel = 100,
  
  // Navigation (Backend cache features)
  canGoBack = false,
  canGoForward = false,
  onGoBack,
  onGoForward,
  
  // Status
  hasHistory = false,
  isHost = false,
  highContrast = false,
  onToggleHighContrast,
  onSettings,
  
  // Backend integration props
  participantCount = 0,
  activeDrawers = 0,
  isSaving = false,
  lastSaved = null,
  isOnline = true,
  backendState = {
    undoCount: 0,
    redoCount: 0,
    checkpoints: [],
    currentCheckpoint: -1
  },
  syncWithBackend,
  sx = {}
}) => {
  const [exportMenuAnchor, setExportMenuAnchor] = useState(null);
  const [isAutoSyncing, setIsAutoSyncing] = useState(false);
  const [syncStatus, setSyncStatus] = useState('synced');
  const [lastSyncTime, setLastSyncTime] = useState(null);
  const [syncError, setSyncError] = useState(null);
  
  // Loading states for async operations
  const [undoLoading, setUndoLoading] = useState(false);
  const [redoLoading, setRedoLoading] = useState(false);
  const [navigationLoading, setNavigationLoading] = useState(false);

  // Debug logging for button states
  useEffect(() => {
    console.log('WhiteboardControls state:', {
      canUndo,
      canRedo,
      backendState,
      onUndo: typeof onUndo,
      onRedo: typeof onRedo,
      isOnline,
      syncStatus
    });
  }, [canUndo, canRedo, backendState, onUndo, onRedo, isOnline, syncStatus]);

  // Auto-sync with backend periodically
  useEffect(() => {
    if (!syncWithBackend || !isOnline) return;
    
    const autoSync = async () => {
      if (isAutoSyncing) return;
      
      setIsAutoSyncing(true);
      setSyncStatus('syncing');
      setSyncError(null);
      
      try {
        await syncWithBackend();
        setSyncStatus('synced');
        setLastSyncTime(new Date());
        setSyncError(null);
      } catch (error) {
        console.error('Auto-sync failed:', error);
        setSyncStatus('error');
        setSyncError(error.message);
      } finally {
        setIsAutoSyncing(false);
      }
    };
    
    // Auto-sync every 30 seconds
    const interval = setInterval(autoSync, 30000);
    
    return () => clearInterval(interval);
  }, [syncWithBackend, isOnline, isAutoSyncing]);

  // FIXED: Handle enhanced undo with proper async handling
  const handleUndo = useCallback(async () => {
    console.log('Undo button clicked', { canUndo, onUndo: typeof onUndo });
    
    if (!canUndo || !onUndo || undoLoading) {
      console.warn('Undo not available:', { canUndo, hasOnUndo: !!onUndo, undoLoading });
      return;
    }
    
    setUndoLoading(true);
    setSyncStatus('syncing');
    setSyncError(null);
    
    try {
      const result = await onUndo();
      
      if (result === false) {
        setSyncError('Undo operation failed');
        setSyncStatus('error');
      } else {
        setSyncStatus('synced');
        setLastSyncTime(new Date());
        setSyncError(null);
      }
    } catch (error) {
      console.error('Undo failed:', error);
      setSyncError(`Undo failed: ${error.message}`);
      setSyncStatus('error');
    } finally {
      setUndoLoading(false);
    }
  }, [canUndo, onUndo, undoLoading]);

  // FIXED: Handle enhanced redo with proper async handling
  const handleRedo = useCallback(async () => {
    console.log('Redo button clicked', { canRedo, onRedo: typeof onRedo });
    
    if (!canRedo || !onRedo || redoLoading) {
      console.warn('Redo not available:', { canRedo, hasOnRedo: !!onRedo, redoLoading });
      return;
    }
    
    setRedoLoading(true);
    setSyncStatus('syncing');
    setSyncError(null);
    
    try {
      const result = await onRedo();
      
      if (result === false) {
        setSyncError('Redo operation failed');
        setSyncStatus('error');
      } else {
        setSyncStatus('synced');
        setLastSyncTime(new Date());
        setSyncError(null);
      }
    } catch (error) {
      console.error('Redo failed:', error);
      setSyncError(`Redo failed: ${error.message}`);
      setSyncStatus('error');
    } finally {
      setRedoLoading(false);
    }
  }, [canRedo, onRedo, redoLoading]);

  // Handle navigation backward with loading state
  const handleGoBack = useCallback(async () => {
    if (!canGoBack || !onGoBack || navigationLoading) return;
    
    setNavigationLoading(true);
    setSyncStatus('syncing');
    setSyncError(null);
    
    try {
      const result = await onGoBack();
      
      if (result === false) {
        setSyncError('Navigation failed');
        setSyncStatus('error');
      } else {
        setSyncStatus('synced');
        setLastSyncTime(new Date());
        setSyncError(null);
      }
    } catch (error) {
      console.error('Go back failed:', error);
      setSyncError(`Navigation failed: ${error.message}`);
      setSyncStatus('error');
    } finally {
      setNavigationLoading(false);
    }
  }, [canGoBack, onGoBack, navigationLoading]);

  // Handle navigation forward with loading state
  const handleGoForward = useCallback(async () => {
    if (!canGoForward || !onGoForward || navigationLoading) return;
    
    setNavigationLoading(true);
    setSyncStatus('syncing');
    setSyncError(null);
    
    try {
      const result = await onGoForward();
      
      if (result === false) {
        setSyncError('Navigation failed');
        setSyncStatus('error');
      } else {
        setSyncStatus('synced');
        setLastSyncTime(new Date());
        setSyncError(null);
      }
    } catch (error) {
      console.error('Go forward failed:', error);
      setSyncError(`Navigation failed: ${error.message}`);
      setSyncStatus('error');
    } finally {
      setNavigationLoading(false);
    }
  }, [canGoForward, onGoForward, navigationLoading]);

  // Handle export menu
  const handleExportClick = (event) => {
    setExportMenuAnchor(event.currentTarget);
  };

  const handleExportClose = () => {
    setExportMenuAnchor(null);
  };

  // Handle manual sync
  const handleManualSync = async () => {
    if (!syncWithBackend || isAutoSyncing) return;
    
    setIsAutoSyncing(true);
    setSyncStatus('syncing');
    setSyncError(null);
    
    try {
      await syncWithBackend();
      setSyncStatus('synced');
      setLastSyncTime(new Date());
      setSyncError(null);
    } catch (error) {
      console.error('Manual sync failed:', error);
      setSyncStatus('error');
      setSyncError(error.message);
    } finally {
      setIsAutoSyncing(false);
    }
  };

  // Format time ago
  const formatTimeAgo = (timestamp) => {
    if (!timestamp) return 'Never';
    
    const now = new Date();
    const time = new Date(timestamp);
    const diff = now - time;
    
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) return `${hours}h ago`;
    if (minutes > 0) return `${minutes}m ago`;
    return `${seconds}s ago`;
  };

  // Sync status indicator
  const getSyncStatusIcon = () => {
    switch (syncStatus) {
      case 'syncing':
        return <CloudSync fontSize="small" sx={{ color: 'warning.main' }} />;
      case 'error':
        return <Error fontSize="small" sx={{ color: 'error.main' }} />;
      case 'synced':
        return <CheckCircle fontSize="small" sx={{ color: 'success.main' }} />;
      default:
        return <CloudSync fontSize="small" />;
    }
  };

  // Enhanced button styling with proper contrast
  const getButtonStyles = (isSelected = false, isDisabled = false, variant = 'default') => {
    const baseStyles = {
      color: highContrast ? '#ffffff' : '#212529',
      backgroundColor: highContrast ? '#333333' : '#ffffff',
      border: `2px solid ${highContrast ? '#999999' : '#6c757d'}`,
      boxShadow: highContrast ? 'none' : '0 2px 4px rgba(0,0,0,0.1)',
      '&:hover': {
        backgroundColor: highContrast ? '#555555' : '#f8f9fa',
        borderColor: highContrast ? '#bbbbbb' : '#495057',
      },
    };

    if (isSelected) {
      baseStyles.backgroundColor = highContrast ? '#ffffff' : '#007bff';
      baseStyles.color = highContrast ? '#000000' : '#ffffff';
      baseStyles.borderColor = highContrast ? '#ffffff' : '#007bff';
    }

    if (isDisabled) {
      baseStyles.color = highContrast ? '#666666' : '#6c757d';
      baseStyles.backgroundColor = highContrast ? '#2a2a2a' : '#e9ecef';
      baseStyles.borderColor = highContrast ? '#555555' : '#adb5bd';
    }

    // Color variants for specific actions
    switch (variant) {
      case 'success':
        return {
          ...baseStyles,
          color: '#28a745',
          backgroundColor: highContrast ? 'rgba(40,167,69,0.1)' : 'rgba(40,167,69,0.05)',
          borderColor: highContrast ? '#28a745' : 'rgba(40,167,69,0.3)',
          '&:hover': {
            backgroundColor: 'rgba(40,167,69,0.15)',
            borderColor: '#28a745',
          },
        };
      case 'danger':
        return {
          ...baseStyles,
          color: '#dc3545',
          backgroundColor: highContrast ? 'rgba(220,53,69,0.1)' : 'rgba(220,53,69,0.05)',
          borderColor: highContrast ? '#dc3545' : 'rgba(220,53,69,0.3)',
          '&:hover': {
            backgroundColor: 'rgba(220,53,69,0.15)',
            borderColor: '#dc3545',
          },
        };
      default:
        return baseStyles;
    }
  };

  return (
    <Paper
      elevation={2}
      sx={{
        p: 2,
        backgroundColor: highContrast ? '#1a1a1a' : '#f9f9f9',
        color: highContrast ? '#ffffff' : 'inherit',
        borderRadius: 2,
        border: highContrast ? '2px solid #ffffff' : '1px solid #e9ecef',
        ...sx
      }}
    >
      {/* Sync Error Alert */}
      {syncError && (
        <Alert 
          severity="error" 
          onClose={() => setSyncError(null)}
          sx={{ mb: 2 }}
        >
          {syncError}
        </Alert>
      )}

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
        
        {/* History Controls Section */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography 
            variant="caption" 
            sx={{ 
              color: highContrast ? '#cccccc' : '#6c757d', 
              minWidth: 45,
              fontWeight: '600',
              fontSize: '0.8rem'
            }}
          >
            History
          </Typography>
          
          <ButtonGroup size="small" variant="outlined">
            <Tooltip title={`Undo (${backendState.undoCount || 0} available)`}>
              <span>
                <IconButton 
                  onClick={handleUndo} 
                  disabled={!canUndo || undoLoading || isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, !canUndo || undoLoading || isSaving || isAutoSyncing)}
                >
                  {undoLoading ? (
                    <CircularProgress size={16} />
                  ) : (
                    <Undo fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>
            
            <Tooltip title={`Redo (${backendState.redoCount || 0} available)`}>
              <span>
                <IconButton 
                  onClick={handleRedo} 
                  disabled={!canRedo || redoLoading || isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, !canRedo || redoLoading || isSaving || isAutoSyncing)}
                >
                  {redoLoading ? (
                    <CircularProgress size={16} />
                  ) : (
                    <Redo fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>
          </ButtonGroup>
        </Box>

        <Divider orientation="vertical" flexItem sx={{ 
          bgcolor: highContrast ? '#999999' : '#dee2e6',
          width: '2px'
        }} />

        {/* Navigation Controls Section */}
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
            Navigate
          </Typography>
          
          <ButtonGroup size="small" variant="outlined">
            <Tooltip title="Go Back in History">
              <span>
                <IconButton 
                  onClick={handleGoBack} 
                  disabled={!canGoBack || navigationLoading || isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, !canGoBack || navigationLoading || isSaving || isAutoSyncing)}
                >
                  {navigationLoading ? (
                    <CircularProgress size={16} />
                  ) : (
                    <NavigateBefore fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>
            
            <Tooltip title="Go Forward in History">
              <span>
                <IconButton 
                  onClick={handleGoForward} 
                  disabled={!canGoForward || navigationLoading || isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, !canGoForward || navigationLoading || isSaving || isAutoSyncing)}
                >
                  {navigationLoading ? (
                    <CircularProgress size={16} />
                  ) : (
                    <NavigateNext fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>
          </ButtonGroup>

          {/* Navigation Status */}
          <Typography 
            variant="caption" 
            sx={{ 
              color: highContrast ? '#cccccc' : '#6c757d',
              mx: 1,
              fontWeight: '500'
            }}
          >
            {backendState.currentCheckpoint + 1}/{backendState.checkpoints?.length || 0}
          </Typography>
        </Box>

        <Divider orientation="vertical" flexItem sx={{ 
          bgcolor: highContrast ? '#999999' : '#dee2e6',
          width: '2px'
        }} />

        {/* Action Controls */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography 
            variant="caption" 
            sx={{ 
              color: highContrast ? '#cccccc' : '#6c757d', 
              minWidth: 45,
              fontWeight: '600',
              fontSize: '0.8rem'
            }}
          >
            Actions
          </Typography>
          
          <ButtonGroup size="small" variant="outlined">
            <Tooltip title="Clear All">
              <IconButton 
                onClick={onClear}
                disabled={isSaving || isAutoSyncing}
                sx={getButtonStyles(false, isSaving || isAutoSyncing, 'danger')}
              >
                <Clear fontSize="small" />
              </IconButton>
            </Tooltip>
            
            <Tooltip title={isSaving ? 'Saving...' : 'Save to Backend Cache'}>
              <span>
                <IconButton 
                  onClick={onSave}
                  disabled={isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, isSaving || isAutoSyncing, 'success')}
                >
                  {isSaving ? (
                    <CircularProgress size={16} />
                  ) : (
                    <Save fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>
            
            <Tooltip title="Export Options">
              <IconButton 
                onClick={handleExportClick}
                disabled={isSaving || isAutoSyncing}
                sx={getButtonStyles(Boolean(exportMenuAnchor), isSaving || isAutoSyncing)}
              >
                <Download fontSize="small" />
              </IconButton>
            </Tooltip>
            
            {isHost && onImport && (
              <Tooltip title="Import Image (Host Only)">
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
                  disabled={isSaving || isAutoSyncing}
                  sx={getButtonStyles(false, isSaving || isAutoSyncing, 'success')}
                >
                  <Upload fontSize="small" />
                </IconButton>
              </Tooltip>
            )}
          </ButtonGroup>
        </Box>

        <Divider orientation="vertical" flexItem sx={{ 
          bgcolor: highContrast ? '#999999' : '#dee2e6',
          width: '2px'
        }} />

        {/* Status and Sync Section */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexGrow: 1, justifyContent: 'space-between' }}>
          
          {/* Status Chips */}
          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
            {participantCount > 0 && (
              <Chip
                label={`${participantCount} participants`}
                size="small"
                color="primary"
                sx={{
                  backgroundColor: highContrast ? 'rgba(25,118,210,0.1)' : undefined,
                  color: highContrast ? '#ffffff' : undefined,
                  fontWeight: '600'
                }}
              />
            )}
            
            {lastSaved && (
              <Chip
                label={`Saved ${formatTimeAgo(lastSaved)}`}
                size="small"
                sx={{ 
                  backgroundColor: highContrast ? 'rgba(40,167,69,0.1)' : 'rgba(40,167,69,0.1)',
                  color: highContrast ? '#aaffaa' : '#28a745',
                  border: `1px solid ${highContrast ? '#28a745' : 'rgba(40,167,69,0.3)'}`,
                  fontWeight: '600'
                }}
              />
            )}

            {highContrast && (
              <Chip
                label="High Contrast"
                size="small"
                sx={{ 
                  backgroundColor: '#ffffff', 
                  color: '#000000',
                  fontWeight: '600',
                  border: '1px solid #333333'
                }}
              />
            )}

            {isFullscreen && (
              <Chip
                label="FULLSCREEN"
                size="small"
                sx={{ 
                  backgroundColor: '#ff9800', 
                  color: '#000000',
                  fontWeight: '600',
                  border: '1px solid #e68900'
                }}
              />
            )}
          </Box>

          {/* Sync Status and Controls */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {/* Sync Status Indicator */}
            <Tooltip title={`Sync Status: ${syncStatus} ${lastSyncTime ? `(${formatTimeAgo(lastSyncTime)})` : ''}`}>
              <Box sx={{ 
                display: 'flex', 
                alignItems: 'center', 
                gap: 0.5,
                p: 1,
                backgroundColor: highContrast ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.02)',
                borderRadius: 2,
                border: `1px solid ${highContrast ? '#666666' : '#e9ecef'}`,
              }}>
                {getSyncStatusIcon()}
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: highContrast ? '#cccccc' : '#6c757d',
                    fontWeight: '600'
                  }}
                >
                  {isOnline ? (
                    syncStatus === 'syncing' || isAutoSyncing ? 'Syncing...' : 
                    syncStatus === 'error' ? 'Sync Error' : 'Synced'
                  ) : 'Offline'}
                </Typography>
              </Box>
            </Tooltip>

            {/* Manual Sync Button */}
            <Tooltip title="Manual Sync">
              <span>
                <IconButton
                  onClick={handleManualSync}
                  disabled={isAutoSyncing || !isOnline}
                  size="small"
                  sx={getButtonStyles(false, isAutoSyncing || !isOnline)}
                >
                  {isAutoSyncing ? (
                    <CircularProgress size={16} />
                  ) : (
                    <Refresh fontSize="small" />
                  )}
                </IconButton>
              </span>
            </Tooltip>

            {/* Settings */}
            {onSettings && (
              <Tooltip title="Settings">
                <IconButton 
                  onClick={onSettings}
                  size="small"
                  sx={getButtonStyles(false, false)}
                >
                  <Settings fontSize="small" />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        </Box>
      </Box>

      {/* Export Menu */}
      <Menu
        anchorEl={exportMenuAnchor}
        open={Boolean(exportMenuAnchor)}
        onClose={handleExportClose}
        PaperProps={{
          sx: { 
            bgcolor: highContrast ? '#1a1a1a' : '#ffffff',
            color: highContrast ? '#ffffff' : 'inherit',
            border: `2px solid ${highContrast ? '#999999' : '#e9ecef'}`,
          }
        }}
      >
        <MenuItem onClick={() => { onExportPNG && onExportPNG(); handleExportClose(); }}>
          <ListItemIcon sx={{ color: highContrast ? '#ffffff' : 'inherit' }}>
            <Image fontSize="small" />
          </ListItemIcon>
          <ListItemText>Export as PNG</ListItemText>
        </MenuItem>
        
        <MenuItem onClick={() => { onExportSVG && onExportSVG(); handleExportClose(); }}>
          <ListItemIcon sx={{ color: highContrast ? '#ffffff' : 'inherit' }}>
            <GetApp fontSize="small" />
          </ListItemIcon>
          <ListItemText>Export as SVG</ListItemText>
        </MenuItem>
      </Menu>

      {/* Connection Status Alert */}
      {!isOnline && (
        <Box sx={{ mt: 1 }}>
          <Alert 
            severity="warning" 
            size="small"
            sx={{ 
              backgroundColor: highContrast ? 'rgba(255,193,7,0.1)' : undefined,
              color: highContrast ? '#ffffff' : undefined,
              border: highContrast ? '1px solid #ffc107' : undefined
            }}
          >
            You are working offline. Changes will be synced when connection is restored.
          </Alert>
        </Box>
      )}

    </Paper>
  );
};

export default WhiteboardControls;