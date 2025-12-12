// src/components/dialogs/RecordingNameDialog.jsx
import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Box,
  Typography,
  CircularProgress,
} from '@mui/material';
import { styled } from '@mui/material/styles';
import SaveIcon from '@mui/icons-material/Save';
import CloseIcon from '@mui/icons-material/Close';

const StyledDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    borderRadius: '16px',
    padding: theme.spacing(2),
    minWidth: '450px',
    maxWidth: '500px',
  },
}));

const RecordingNameDialog = ({ 
  open, 
  onClose, 
  onSave, 
  defaultName = '',
  loading = false 
}) => {
  const [recordingName, setRecordingName] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    if (open) {
      // Generate default name with timestamp
      const timestamp = new Date().toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).replace(/[/:]/g, '-').replace(/, /g, '_');
      
      const defaultRecordingName = defaultName || `Recording_${timestamp}`;
      setRecordingName(defaultRecordingName);
      setError('');
    }
  }, [open, defaultName]);

  const handleSave = () => {
    if (!recordingName.trim()) {
      setError('Please enter a recording name');
      return;
    }

    // Validate name (no special characters except underscores and hyphens)
    const validNameRegex = /^[a-zA-Z0-9_\-\s]+$/;
    if (!validNameRegex.test(recordingName)) {
      setError('Recording name can only contain letters, numbers, spaces, underscores, and hyphens');
      return;
    }

    onSave(recordingName.trim());
  };

  const handleCancel = () => {
    // Save with default name
    const timestamp = new Date().toLocaleString('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).replace(/[/:]/g, '-').replace(/, /g, '_');
    
    const defaultRecordingName = `Recording_${timestamp}`;
    onSave(defaultRecordingName);
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !loading) {
      handleSave();
    }
  };

  return (
    <StyledDialog
      open={open}
      onClose={loading ? undefined : handleCancel}
      disableEscapeKeyDown={loading}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle sx={{ 
        textAlign: 'center', 
        fontWeight: 600, 
        fontSize: '1.5rem',
        pb: 1 
      }}>
        📹 Name Your Recording
      </DialogTitle>

      <DialogContent>
        <Box sx={{ pt: 2 }}>
          <Typography 
            variant="body2" 
            color="text.secondary" 
            sx={{ mb: 2, textAlign: 'center' }}
          >
            Enter a name for your recording to help you find it later
          </Typography>

          <TextField
            autoFocus
            fullWidth
            label="Recording Name"
            variant="outlined"
            value={recordingName}
            onChange={(e) => {
              setRecordingName(e.target.value);
              setError('');
            }}
            onKeyPress={handleKeyPress}
            error={!!error}
            helperText={error || 'Only letters, numbers, spaces, underscores, and hyphens allowed'}
            disabled={loading}
            placeholder="e.g., Team Meeting March 2024"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: '8px',
              },
            }}
          />
        </Box>
      </DialogContent>

      <DialogActions sx={{ justifyContent: 'center', gap: 2, pb: 2, pt: 1 }}>
        <Button
          onClick={handleCancel}
          disabled={loading}
          variant="outlined"
          startIcon={<CloseIcon />}
          sx={{
            minWidth: '120px',
            borderRadius: '8px',
            textTransform: 'none',
            fontSize: '1rem',
          }}
        >
          Use Default
        </Button>
        <Button
          onClick={handleSave}
          disabled={loading}
          variant="contained"
          startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <SaveIcon />}
          sx={{
            minWidth: '120px',
            borderRadius: '8px',
            textTransform: 'none',
            fontSize: '1rem',
          }}
        >
          {loading ? 'Saving...' : 'Save'}
        </Button>
      </DialogActions>
    </StyledDialog>
  );
};

export default RecordingNameDialog;