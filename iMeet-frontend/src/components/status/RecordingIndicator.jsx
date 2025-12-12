// src/components/status/RecordingIndicator.jsx
import React from 'react';
import { Chip } from '@mui/material';
import { RadioButtonChecked } from '@mui/icons-material';

const RecordingIndicator = ({ 
  isRecording, 
  recordingMethod, 
  duration,
  uploading,
  uploadProgress 
}) => {
  if (!isRecording) return null;

  const formatDuration = (seconds) => {
    if (!seconds || seconds <= 0) return "0:00";

    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;

    if (hrs > 0) {
      return `${hrs}:${mins.toString().padStart(2, "0")}:${secs
        .toString()
        .padStart(2, "0")}`;
    }
    return `${mins}:${secs.toString().padStart(2, "0")}`;
  };

  const getLabel = () => {
    if (uploading) {
      return `Uploading... ${uploadProgress}%`;
    }

    const formattedDuration = formatDuration(duration);

    if (recordingMethod === "client") {
      return `REC (Browser) ${formattedDuration}`;
    } else if (recordingMethod === "server") {
      return `REC (Server) ${formattedDuration}`;
    }

    return `REC ${formattedDuration}`;
  };

  return (
    <Chip
      icon={<RadioButtonChecked />}
      label={getLabel()}
      sx={{
        height: 26,
        backgroundColor: '#ef4444',
        color: 'white',
        fontWeight: 500,
        fontSize: '12px',
        animation: 'pulse 2s infinite',
        '@keyframes pulse': {
          '0%': { opacity: 1 },
          '50%': { opacity: 0.8 },
          '100%': { opacity: 1 },
        },
        '& .MuiChip-icon': {
          color: 'white',
          fontSize: 12,
        }
      }}
    />
  );
};

export default RecordingIndicator;