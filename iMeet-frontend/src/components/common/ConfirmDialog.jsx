import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  IconButton,
  Slide,
  useTheme,
  useMediaQuery
} from '@mui/material';
import {
  Close,
  Warning,
  Error,
  Delete,
  ExitToApp,
  Save,
  Cancel,
  CheckCircle,
  Info,
  HelpOutline
} from '@mui/icons-material';
import { forwardRef } from 'react';

const Transition = forwardRef(function Transition(props, ref) {
  return <Slide direction="up" ref={ref} {...props} />;
});

const ConfirmDialog = ({
  open,
  onClose,
  onConfirm,
  title,
  message,
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  type = 'default',
  severity = 'medium',
  showIcon = true,
  maxWidth = 'sm',
  fullWidth = true,
  children,
  confirmColor = 'primary',
  cancelColor = 'inherit',
  disabled = false,
  loading = false
}) => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  const getIcon = () => {
    switch (type) {
      case 'delete':
        return <Delete sx={{ fontSize: 48, color: 'error.main' }} />;
      case 'warning':
        return <Warning sx={{ fontSize: 48, color: 'warning.main' }} />;
      case 'error':
        return <Error sx={{ fontSize: 48, color: 'error.main' }} />;
      case 'leave':
        return <ExitToApp sx={{ fontSize: 48, color: 'warning.main' }} />;
      case 'save':
        return <Save sx={{ fontSize: 48, color: 'success.main' }} />;
      case 'success':
        return <CheckCircle sx={{ fontSize: 48, color: 'success.main' }} />;
      case 'info':
        return <Info sx={{ fontSize: 48, color: 'info.main' }} />;
      case 'question':
        return <HelpOutline sx={{ fontSize: 48, color: 'primary.main' }} />;
      default:
        return <Warning sx={{ fontSize: 48, color: 'primary.main' }} />;
    }
  };

  const getConfirmColor = () => {
    if (confirmColor !== 'primary') return confirmColor;
    
    switch (type) {
      case 'delete':
      case 'error':
        return 'error';
      case 'warning':
      case 'leave':
        return 'warning';
      case 'save':
      case 'success':
        return 'success';
      default:
        return 'primary';
    }
  };

  const handleConfirm = () => {
    if (onConfirm) {
      onConfirm();
    }
  };

  const handleClose = () => {
    if (onClose) {
      onClose();
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth={maxWidth}
      fullWidth={fullWidth}
      TransitionComponent={Transition}
      PaperProps={{
        sx: {
          borderRadius: 3,
          minHeight: 200,
          background: theme.palette.mode === 'dark' 
            ? 'linear-gradient(145deg, #2d2d30 0%, #1a1a1a 100%)'
            : 'linear-gradient(145deg, #ffffff 0%, #f5f5f5 100%)',
          boxShadow: '0 20px 40px rgba(0,0,0,0.1)',
        }
      }}
    >
      {/* Close Button */}
      <IconButton
        onClick={handleClose}
        sx={{
          position: 'absolute',
          right: 8,
          top: 8,
          color: 'text.secondary',
          '&:hover': {
            bgcolor: 'action.hover',
            transform: 'scale(1.1)'
          }
        }}
      >
        <Close />
      </IconButton>

      {/* Dialog Title */}
      <DialogTitle sx={{ 
        textAlign: 'center', 
        pt: 3, 
        pb: 1,
        pr: 6 // Account for close button
      }}>
        {showIcon && (
          <Box sx={{ mb: 2, display: 'flex', justifyContent: 'center' }}>
            {getIcon()}
          </Box>
        )}
        
        <Typography 
          variant="h5" 
          component="h2" 
          fontWeight="bold"
          sx={{
            color: type === 'delete' || type === 'error' ? 'error.main' :
                   type === 'warning' || type === 'leave' ? 'warning.main' :
                   type === 'success' ? 'success.main' : 'text.primary'
          }}
        >
          {title}
        </Typography>
      </DialogTitle>

      {/* Dialog Content */}
      <DialogContent sx={{ 
        textAlign: 'center', 
        pb: 2,
        px: isMobile ? 2 : 3
      }}>
        {message && (
          <Typography 
            variant="body1" 
            color="text.secondary"
            sx={{ 
              mb: children ? 2 : 0,
              lineHeight: 1.6,
              fontSize: '1.1rem'
            }}
          >
            {message}
          </Typography>
        )}
        
        {children && (
          <Box sx={{ mt: 2 }}>
            {children}
          </Box>
        )}
      </DialogContent>

      {/* Dialog Actions */}
      <DialogActions sx={{ 
        justifyContent: 'center', 
        gap: 2, 
        pb: 3,
        px: 3
      }}>
        <Button
          onClick={handleClose}
          color={cancelColor}
          variant="outlined"
          size="large"
          sx={{
            minWidth: 120,
            height: 48,
            borderRadius: 2,
            textTransform: 'none',
            fontSize: '1rem',
            fontWeight: 500
          }}
        >
          {cancelText}
        </Button>
        
        <Button
          onClick={handleConfirm}
          color={getConfirmColor()}
          variant="contained"
          size="large"
          disabled={disabled || loading}
          sx={{
            minWidth: 120,
            height: 48,
            borderRadius: 2,
            textTransform: 'none',
            fontSize: '1rem',
            fontWeight: 600,
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            '&:hover': {
              boxShadow: '0 6px 16px rgba(0,0,0,0.2)',
              transform: 'translateY(-1px)'
            }
          }}
        >
          {loading ? 'Processing...' : confirmText}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

// Predefined Dialog Types
export const DeleteConfirmDialog = ({ open, onClose, onConfirm, itemName, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="delete"
    title="Delete Item"
    message={`Are you sure you want to delete ${itemName || 'this item'}? This action cannot be undone.`}
    confirmText="Delete"
    cancelText="Cancel"
    {...props}
  />
);

export const LeaveMeetingDialog = ({ open, onClose, onConfirm, meetingName, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="leave"
    title="Leave Meeting"
    message={`Are you sure you want to leave ${meetingName || 'this meeting'}?`}
    confirmText="Leave"
    cancelText="Stay"
    {...props}
  />
);

export const EndMeetingDialog = ({ open, onClose, onConfirm, meetingName, participantCount, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="warning"
    title="End Meeting"
    message={`Are you sure you want to end ${meetingName || 'this meeting'}? ${participantCount ? `All ${participantCount} participants will be removed.` : 'All participants will be removed.'}`}
    confirmText="End Meeting"
    cancelText="Cancel"
    {...props}
  />
);

export const SaveChangesDialog = ({ open, onClose, onConfirm, hasUnsavedChanges, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="save"
    title="Save Changes"
    message={hasUnsavedChanges ? "You have unsaved changes. Do you want to save them before leaving?" : "Do you want to save your changes?"}
    confirmText="Save"
    cancelText="Don't Save"
    {...props}
  />
);

export const RemoveParticipantDialog = ({ open, onClose, onConfirm, participantName, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="warning"
    title="Remove Participant"
    message={`Are you sure you want to remove ${participantName || 'this participant'} from the meeting?`}
    confirmText="Remove"
    cancelText="Cancel"
    {...props}
  />
);

export const CancelMeetingDialog = ({ open, onClose, onConfirm, meetingName, isScheduled, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="error"
    title="Cancel Meeting"
    message={`Are you sure you want to cancel ${meetingName || 'this meeting'}? ${isScheduled ? 'Participants will be notified of the cancellation.' : ''}`}
    confirmText="Cancel Meeting"
    cancelText="Keep Meeting"
    {...props}
  />
);

export const LogoutDialog = ({ open, onClose, onConfirm, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="question"
    title="Sign Out"
    message="Are you sure you want to sign out of your account?"
    confirmText="Sign Out"
    cancelText="Stay Signed In"
    {...props}
  />
);

export const ClearDataDialog = ({ open, onClose, onConfirm, dataType = 'data', ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="warning"
    title="Clear Data"
    message={`Are you sure you want to clear all ${dataType}? This action cannot be undone.`}
    confirmText="Clear"
    cancelText="Cancel"
    {...props}
  />
);

export const PermissionDialog = ({ open, onClose, onConfirm, permission, ...props }) => (
  <ConfirmDialog
    open={open}
    onClose={onClose}
    onConfirm={onConfirm}
    type="info"
    title="Permission Required"
    message={`This action requires ${permission || 'additional permissions'}. Do you want to continue?`}
    confirmText="Grant Permission"
    cancelText="Cancel"
    {...props}
  />
);

// Hook for managing multiple dialogs
export const useConfirmDialog = () => {
  const [dialogs, setDialogs] = React.useState({});

  const showDialog = (id, config) => {
    setDialogs(prev => ({
      ...prev,
      [id]: { ...config, open: true }
    }));
  };

  const hideDialog = (id) => {
    setDialogs(prev => ({
      ...prev,
      [id]: { ...prev[id], open: false }
    }));
  };

  const confirmDialog = (id, callback) => {
    const dialog = dialogs[id];
    if (dialog && dialog.onConfirm) {
      dialog.onConfirm();
    }
    if (callback) {
      callback();
    }
    hideDialog(id);
  };

  return {
    dialogs,
    showDialog,
    hideDialog,
    confirmDialog
  };
};

export default ConfirmDialog;