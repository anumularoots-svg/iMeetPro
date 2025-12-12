// src/components/common/DeviceTroubleshooting.jsx - Enhanced Device Management

import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Alert,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  IconButton,
  Tooltip,
  LinearProgress,
  Card,
  CardContent
} from '@mui/material';
import {
  Videocam,
  VideocamOff,
  Mic,
  MicOff,
  Refresh,
  CheckCircle,
  Error,
  Warning,
  ExpandMore,
  Settings,
  Help,
  Computer,
  Security
} from '@mui/icons-material';
import webrtcService from '../../services/webrtc';

const DeviceTroubleshooting = ({ open, onClose, onRetry }) => {
  const [devices, setDevices] = useState({ videoInputs: [], audioInputs: [], audioOutputs: [] });
  const [permissions, setPermissions] = useState({ camera: 'unknown', microphone: 'unknown' });
  const [testStream, setTestStream] = useState(null);
  const [testing, setTesting] = useState(false);
  const [testResults, setTestResults] = useState({ video: null, audio: null });
  const [troubleshootingSteps, setTroubleshootingSteps] = useState([]);

  useEffect(() => {
    if (open) {
      checkDevicesAndPermissions();
      generateTroubleshootingSteps();
    }
    
    return () => {
      if (testStream) {
        testStream.getTracks().forEach(track => track.stop());
      }
    };
  }, [open]);

  const checkDevicesAndPermissions = async () => {
    try {
      // Check permissions
      if (navigator.permissions) {
        const cameraPermission = await navigator.permissions.query({ name: 'camera' });
        const microphonePermission = await navigator.permissions.query({ name: 'microphone' });
        
        setPermissions({
          camera: cameraPermission.state,
          microphone: microphonePermission.state
        });
      }

      // Enumerate devices
      const deviceList = await webrtcService.enumerateDevices();
      setDevices(deviceList);

    } catch (error) {
      console.error('Error checking devices and permissions:', error);
    }
  };

  const generateTroubleshootingSteps = () => {
    const steps = [
      {
        id: 1,
        title: 'Check Browser Compatibility',
        description: 'Ensure you\'re using a modern browser that supports WebRTC',
        status: 'info',
        details: [
          'Chrome 50+, Firefox 50+, Safari 11+, Edge 12+ are recommended',
          'WebRTC is not supported in private/incognito mode in some browsers',
          'Try using a different browser if issues persist'
        ]
      },
      {
        id: 2,
        title: 'Grant Camera and Microphone Permissions',
        description: 'Allow access to your camera and microphone',
        status: permissions.camera === 'granted' && permissions.microphone === 'granted' ? 'success' : 'error',
        details: [
          'Click the camera/microphone icon in your browser\'s address bar',
          'Select "Allow" for both camera and microphone',
          'Refresh the page after granting permissions'
        ]
      },
      {
        id: 3,
        title: 'Check Device Availability',
        description: 'Ensure your camera and microphone are not being used by other applications',
        status: devices.videoInputs.length > 0 && devices.audioInputs.length > 0 ? 'success' : 'warning',
        details: [
          'Close other video calling applications (Zoom, Teams, Skype, etc.)',
          'Close other browser tabs that might be using your camera',
          'Restart your browser if necessary'
        ]
      },
      {
        id: 4,
        title: 'Test Hardware Connection',
        description: 'Verify your camera and microphone are properly connected',
        status: 'info',
        details: [
          'Check that your camera is properly plugged in',
          'Try a different USB port for external cameras',
          'Test your devices in other applications (like your system\'s camera app)'
        ]
      },
      {
        id: 5,
        title: 'Update Drivers and Software',
        description: 'Ensure your hardware drivers are up to date',
        status: 'info',
        details: [
          'Update your camera and audio drivers',
          'Update your browser to the latest version',
          'Restart your computer after updates'
        ]
      }
    ];

    setTroubleshootingSteps(steps);
  };

  const testDevices = async () => {
    setTesting(true);
    setTestResults({ video: null, audio: null });

    try {
      // Test video first
      console.log('Testing video device...');
      try {
        const videoStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
        setTestResults(prev => ({ ...prev, video: 'success' }));
        videoStream.getTracks().forEach(track => track.stop());
      } catch (videoError) {
        console.error('Video test failed:', videoError);
        setTestResults(prev => ({ ...prev, video: videoError.message }));
      }

      // Test audio
      console.log('Testing audio device...');
      try {
        const audioStream = await navigator.mediaDevices.getUserMedia({ video: false, audio: true });
        setTestResults(prev => ({ ...prev, audio: 'success' }));
        audioStream.getTracks().forEach(track => track.stop());
      } catch (audioError) {
        console.error('Audio test failed:', audioError);
        setTestResults(prev => ({ ...prev, audio: audioError.message }));
      }

      // Test both together
      console.log('Testing both devices together...');
      try {
        const bothStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        setTestStream(bothStream);
        
        // Update both results to success if we get here
        setTestResults({ video: 'success', audio: 'success' });
      } catch (bothError) {
        console.error('Combined test failed:', bothError);
        // Keep individual results
      }

    } catch (error) {
      console.error('Device testing failed:', error);
    } finally {
      setTesting(false);
    }
  };

  const retryConnection = async () => {
    try {
      setTesting(true);
      
      // Clean up any existing test stream
      if (testStream) {
        testStream.getTracks().forEach(track => track.stop());
        setTestStream(null);
      }

      // Retry device initialization
      await webrtcService.cleanup();
      await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
      
      const newStream = await webrtcService.initializeMedia(true, true);
      
      if (newStream) {
        console.log('✅ Device retry successful');
        if (onRetry) {
          onRetry();
        }
        onClose();
      }
    } catch (error) {
      console.error('❌ Device retry failed:', error);
      setTestResults({
        video: error.message.includes('video') ? error.message : 'Video test failed',
        audio: error.message.includes('audio') ? error.message : 'Audio test failed'
      });
    } finally {
      setTesting(false);
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'success':
        return <CheckCircle color="success" />;
      case 'error':
        return <Error color="error" />;
      case 'warning':
        return <Warning color="warning" />;
      default:
        return <Help color="info" />;
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'success':
        return 'success';
      case 'error':
        return 'error';
      case 'warning':
        return 'warning';
      default:
        return 'info';
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box display="flex" alignItems="center" gap={1}>
          <Settings color="primary" />
          <Typography variant="h6">Device Troubleshooting</Typography>
        </Box>
      </DialogTitle>

      <DialogContent>
        {/* Device Status Overview */}
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Device Status
            </Typography>
            
            <Box display="flex" gap={2} mb={2}>
              <Chip
                icon={<Videocam />}
                label={`Camera: ${devices.videoInputs.length} found`}
                color={devices.videoInputs.length > 0 ? 'success' : 'error'}
                variant="outlined"
              />
              <Chip
                icon={<Mic />}
                label={`Microphone: ${devices.audioInputs.length} found`}
                color={devices.audioInputs.length > 0 ? 'success' : 'error'}
                variant="outlined"
              />
            </Box>

            <Box display="flex" gap={2} mb={2}>
              <Chip
                icon={<Security />}
                label={`Camera Permission: ${permissions.camera}`}
                color={permissions.camera === 'granted' ? 'success' : 'error'}
                variant="outlined"
              />
              <Chip
                icon={<Security />}
                label={`Microphone Permission: ${permissions.microphone}`}
                color={permissions.microphone === 'granted' ? 'success' : 'error'}
                variant="outlined"
              />
            </Box>

            <Box display="flex" gap={2}>
              <Button
                variant="outlined"
                startIcon={<Computer />}
                onClick={testDevices}
                disabled={testing}
              >
                Test Devices
              </Button>
              <Button
                variant="outlined"
                startIcon={<Refresh />}
                onClick={checkDevicesAndPermissions}
              >
                Refresh Status
              </Button>
            </Box>

            {testing && (
              <Box mt={2}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Testing devices...
                </Typography>
                <LinearProgress />
              </Box>
            )}

            {(testResults.video || testResults.audio) && (
              <Box mt={2}>
                <Typography variant="body2" gutterBottom>
                  Test Results:
                </Typography>
                <Box display="flex" gap={1} flexWrap="wrap">
                  {testResults.video && (
                    <Chip
                      icon={testResults.video === 'success' ? <CheckCircle /> : <Error />}
                      label={`Video: ${testResults.video === 'success' ? 'Working' : 'Failed'}`}
                      color={testResults.video === 'success' ? 'success' : 'error'}
                      size="small"
                    />
                  )}
                  {testResults.audio && (
                    <Chip
                      icon={testResults.audio === 'success' ? <CheckCircle /> : <Error />}
                      label={`Audio: ${testResults.audio === 'success' ? 'Working' : 'Failed'}`}
                      color={testResults.audio === 'success' ? 'success' : 'error'}
                      size="small"
                    />
                  )}
                </Box>
              </Box>
            )}
          </CardContent>
        </Card>

        {/* Common Issues Alert */}
        <Alert severity="info" sx={{ mb: 3 }}>
          <Typography variant="body2" fontWeight="bold" gutterBottom>
            Most Common Issues:
          </Typography>
          <Typography variant="body2">
            • Camera already in use by another application or browser tab<br/>
            • Permissions not granted or blocked<br/>
            • Hardware drivers need updating<br/>
            • Browser not compatible or needs updating
          </Typography>
        </Alert>

        {/* Troubleshooting Steps */}
        <Typography variant="h6" gutterBottom>
          Troubleshooting Steps
        </Typography>

        {troubleshootingSteps.map((step) => (
          <Accordion key={step.id}>
            <AccordionSummary expandIcon={<ExpandMore />}>
              <Box display="flex" alignItems="center" gap={2} width="100%">
                {getStatusIcon(step.status)}
                <Box flex={1}>
                  <Typography variant="subtitle1">{step.title}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {step.description}
                  </Typography>
                </Box>
                <Chip
                  label={step.status}
                  color={getStatusColor(step.status)}
                  size="small"
                />
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              <List dense>
                {step.details.map((detail, index) => (
                  <ListItem key={index}>
                    <ListItemIcon>
                      <Typography variant="body2" color="text.secondary">
                        {index + 1}.
                      </Typography>
                    </ListItemIcon>
                    <ListItemText primary={detail} />
                  </ListItem>
                ))}
              </List>
            </AccordionDetails>
          </Accordion>
        ))}

        {/* Browser-Specific Help */}
        <Card sx={{ mt: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Browser-Specific Help
            </Typography>
            
            <Typography variant="body2" paragraph>
              <strong>Chrome:</strong> Look for camera/microphone icons in the address bar. Click to change permissions.
            </Typography>
            
            <Typography variant="body2" paragraph>
              <strong>Firefox:</strong> Check for shield icon in address bar. Go to Settings → Privacy & Security → Permissions.
            </Typography>
            
            <Typography variant="body2" paragraph>
              <strong>Safari:</strong> Go to Safari → Preferences → Websites → Camera/Microphone to manage permissions.
            </Typography>
            
            <Typography variant="body2">
              <strong>Edge:</strong> Click the lock icon in address bar to manage site permissions.
            </Typography>
          </CardContent>
        </Card>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>
          Close
        </Button>
        <Button
          variant="contained"
          onClick={retryConnection}
          disabled={testing}
          startIcon={testing ? <LinearProgress size={20} /> : <Refresh />}
        >
          {testing ? 'Retrying...' : 'Retry Connection'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default DeviceTroubleshooting;