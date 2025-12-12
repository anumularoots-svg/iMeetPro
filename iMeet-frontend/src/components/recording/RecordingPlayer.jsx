import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  IconButton,
  Slider,
  Stack,
  Chip,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  Button,
  Divider,
  Grid,
  Avatar,
  DialogActions,
  Alert,
  CircularProgress,
  Menu,
  MenuItem,
  ListItemIcon,
  Snackbar,
  Select,
  FormControl,
  InputLabel
} from '@mui/material';
import {
  PlayArrow as PlayIcon,
  Pause as PauseIcon,
  VolumeUp as VolumeIcon,
  VolumeOff as VolumeOffIcon,
  Fullscreen as FullscreenIcon,
  FullscreenExit as FullscreenExitIcon,
  Settings as SettingsIcon,
  Subtitles as SubtitlesIcon,
  Forward10 as Forward10Icon,
  Replay10 as Replay10Icon,
  AccessTime as AccessTimeIcon,
  ClosedCaption as ClosedCaptionIcon,
  SubtitlesOff as SubtitlesOffIcon,
  GetApp as GetAppIcon,
  Share as ShareIcon,
  Download as DownloadIcon,
  Language as LanguageIcon,
  BugReport as DebugIcon
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';
import { recordingsAPI } from '../../services/recording';
import { getAvailableSubtitleLanguages } from '../../services/recording';

const RecordingPlayer = ({ recordingData = {} }) => {
  const theme = useTheme();
  const videoRef = useRef(null);
  const containerRef = useRef(null);
  const trackRef = useRef(null);
  
  // Player State
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(recordingData.duration || 0);
  const [volume, setVolume] = useState(1);
  const [isMuted, setIsMuted] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const [actualVideoUrl, setActualVideoUrl] = useState('');
  const [actualThumbnailUrl, setActualThumbnailUrl] = useState('');
  
  // UI State
  const [showSettings, setShowSettings] = useState(false);
  const [showControls, setShowControls] = useState(true);
  const [controlsTimeout, setControlsTimeout] = useState(null);

  // Subtitle State
  const [showSubtitles, setShowSubtitles] = useState(true);
  const [subtitlesAvailable, setSubtitlesAvailable] = useState(false);
  const [subtitlesLoading, setSubtitlesLoading] = useState(false);
  const [availableLanguages, setAvailableLanguages] = useState([]);
  const [selectedLanguage, setSelectedLanguage] = useState('en');
  const [subtitleMenuAnchor, setSubtitleMenuAnchor] = useState(null);
  const [subtitleData, setSubtitleData] = useState('');

  // DEBUG: Add debug state
  const [debugInfo, setDebugInfo] = useState({
    recordingDataKeys: [],
    recordingId: '',
    apiCalls: [],
    errors: []
  });
  const [showDebugDialog, setShowDebugDialog] = useState(false);

  // DEBUG: Log all recording data
  useEffect(() => {
    console.log('🔍 DEBUG: Full recording data received:', recordingData);
    console.log('🔍 DEBUG: Recording data keys:', Object.keys(recordingData));
    console.log('🔍 DEBUG: Recording ID candidates:', {
      id: recordingData.id,
      _id: recordingData._id,
      recordingId: recordingData.recordingId,
      meeting_id: recordingData.meeting_id
    });

    setDebugInfo(prev => ({
      ...prev,
      recordingDataKeys: Object.keys(recordingData),
      recordingId: recordingData.id || recordingData._id || recordingData.recordingId || 'NOT_FOUND',
      recordingData: recordingData
    }));
  }, [recordingData]);

  // Format time
  const formatTime = (seconds) => {
    const hours = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
      return `${hours}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  // Auto-hide controls
  const resetControlsTimeout = () => {
    if (controlsTimeout) clearTimeout(controlsTimeout);
    setShowControls(true);
    const timeout = setTimeout(() => {
      if (isPlaying) setShowControls(false);
    }, 3000);
    setControlsTimeout(timeout);
  };

  // Set up video URLs
  useEffect(() => {
    const userData = JSON.parse(localStorage.getItem('user')) || {};
    const userId = userData.id || localStorage.getItem('user_id') || '';
    const userEmail = userData.email || localStorage.getItem('user_email') || '';

    // DEBUG: Log user data
    console.log('🔍 DEBUG: User data for URLs:', { userData, userId, userEmail });

    const recordingId = recordingData.id || recordingData._id || recordingData.recordingId;
    
    if (!recordingId || !userId || !userEmail) {
      console.warn('⚠️ Missing required data for video URLs:', { recordingId, userId, userEmail });
      return;
    }

    const videoUrl = recordingsAPI.getVideoStreamUrl(recordingId, userEmail, userId);
    const thumbnailUrl = recordingsAPI.getThumbnailUrl(recordingId, userEmail, userId);
    
    console.log('🔍 DEBUG: Generated URLs:', { videoUrl, thumbnailUrl });
    
    setActualVideoUrl(videoUrl);
    setActualThumbnailUrl(thumbnailUrl);
    setHasError(false);
  }, [recordingData]);

  // DEBUG: Enhanced subtitle checking with detailed logging
  useEffect(() => {
    const recordingId = recordingData.id || recordingData._id || recordingData.recordingId;
    if (recordingId) {
      console.log('🎬 DEBUG: Starting subtitle check for ID:', recordingId);
      debugCheckExistingSubtitles(recordingId);
    } else {
      console.error('❌ DEBUG: No valid recording ID found for subtitle check');
      setDebugInfo(prev => ({
        ...prev,
        errors: [...prev.errors, 'No valid recording ID found']
      }));
    }
  }, [recordingData]);

  // DEBUG: Enhanced subtitle checking function
  const debugCheckExistingSubtitles = async (recordingId) => {
    try {
      console.log('🎬 DEBUG: Checking existing subtitles for recording:', recordingId);
      
      setDebugInfo(prev => ({
        ...prev,
        apiCalls: [...prev.apiCalls, `Starting subtitle check for ${recordingId}`]
      }));

      // Step 1: Try to get the recording data first
      console.log('📋 DEBUG: Step 1 - Getting recording data...');
      let recordingResponse;
      try {
        recordingResponse = await recordingsAPI.getRecording(recordingId);
        console.log('✅ DEBUG: Recording data retrieved:', recordingResponse);
        
        setDebugInfo(prev => ({
          ...prev,
          apiCalls: [...prev.apiCalls, `✅ Retrieved recording data`],
          fullRecordingData: recordingResponse
        }));
      } catch (err) {
        console.error('❌ DEBUG: Failed to get recording data:', err);
        setDebugInfo(prev => ({
          ...prev,
          errors: [...prev.errors, `Failed to get recording: ${err.message}`]
        }));
        return;
      }

      // Step 2: Check subtitle availability
      console.log('📋 DEBUG: Step 2 - Checking subtitle availability...');
      const available = await recordingsAPI.checkSubtitlesAvailable(recordingId);
      console.log('📋 DEBUG: Subtitle availability result:', available);
      
      setDebugInfo(prev => ({
        ...prev,
        apiCalls: [...prev.apiCalls, `Subtitle availability: ${available}`]
      }));

      if (available) {
        console.log('✅ DEBUG: Subtitles found! Getting available languages...');
        setSubtitlesAvailable(true);
        
        // Step 3: Get available languages
        const languages = await getAvailableSubtitleLanguages(recordingId);
        console.log('🌐 DEBUG: Available languages:', languages);
        
        setAvailableLanguages(languages);
        setDebugInfo(prev => ({
          ...prev,
          apiCalls: [...prev.apiCalls, `Found ${languages.length} languages: ${languages.map(l => l.name).join(', ')}`],
          availableLanguages: languages
        }));
        
        if (languages.length > 0) {
          // Set default language (prefer English, then first available)
          const defaultLang = languages.find(lang => lang.code === 'en') || languages[0];
          setSelectedLanguage(defaultLang.code);
          console.log('🎯 DEBUG: Selected default language:', defaultLang);
          
          // Step 4: Load subtitles
          await debugLoadSubtitles(recordingId, defaultLang.code);
        }
      } else {
        console.log('ℹ️ DEBUG: No subtitles found in backend');
        setSubtitlesAvailable(false);
        setDebugInfo(prev => ({
          ...prev,
          apiCalls: [...prev.apiCalls, 'No subtitles available in backend']
        }));
      }
    } catch (error) {
      console.error('❌ DEBUG: Failed to check existing subtitles:', error);
      setSubtitlesAvailable(false);
      setDebugInfo(prev => ({
        ...prev,
        errors: [...prev.errors, `Subtitle check failed: ${error.message}`]
      }));
    }
  };

  // DEBUG: Enhanced subtitle loading function
  const debugLoadSubtitles = async (recordingId, language = 'en') => {
    try {
      setSubtitlesLoading(true);
      console.log('📥 DEBUG: Loading subtitles for language:', language);
      
      setDebugInfo(prev => ({
        ...prev,
        apiCalls: [...prev.apiCalls, `Loading subtitles for ${language}`]
      }));
      
      const response = await recordingsAPI.getSubtitles(recordingId, language);
      console.log('📥 DEBUG: Subtitle response:', response);
      
      if (response && response.subtitles) {
        setSubtitleData(response.subtitles);
        console.log('✅ DEBUG: Subtitle data loaded, length:', response.subtitles.length);
        
        setDebugInfo(prev => ({
          ...prev,
          apiCalls: [...prev.apiCalls, `✅ Loaded subtitles (${response.subtitles.length} chars)`],
          subtitleContent: response.subtitles.substring(0, 200) + '...'
        }));
        
        // Convert SRT to WebVTT if needed and add to video
        if (videoRef.current) {
          await addSubtitleTrack(response.subtitles, language);
        }
        
        console.log('✅ DEBUG: Subtitles loaded successfully for language:', language);
      } else {
        console.log('⚠️ DEBUG: No subtitle content in response');
        setDebugInfo(prev => ({
          ...prev,
          errors: [...prev.errors, 'No subtitle content in API response']
        }));
      }
    } catch (error) {
      console.error('❌ DEBUG: Failed to load subtitles for language:', language, error);
      setDebugInfo(prev => ({
        ...prev,
        errors: [...prev.errors, `Failed to load subtitles for ${language}: ${error.message}`]
      }));
    } finally {
      setSubtitlesLoading(false);
    }
  };

  // Add subtitle track to video element
  const addSubtitleTrack = async (subtitleContent, language) => {
    if (!videoRef.current || !subtitleContent) return;

    try {
      console.log('🎬 DEBUG: Adding subtitle track for language:', language);
      
      // Remove existing track
      const existingTrack = trackRef.current;
      if (existingTrack) {
        videoRef.current.removeChild(existingTrack);
      }

      // Convert SRT to WebVTT if needed
      let vttContent = subtitleContent;
      if (!subtitleContent.startsWith('WEBVTT')) {
        vttContent = convertSRTtoWebVTT(subtitleContent);
        console.log('🔄 DEBUG: Converted SRT to WebVTT');
      }

      // Create new track element
      const track = document.createElement('track');
      track.kind = 'subtitles';
      track.label = getLanguageName(language);
      track.srclang = language;
      track.default = true;

      // Create blob URL for subtitle content
      const blob = new Blob([vttContent], { type: 'text/vtt' });
      track.src = URL.createObjectURL(blob);

      // Add track to video
      videoRef.current.appendChild(track);
      trackRef.current = track;

      // Set visibility based on showSubtitles state
      track.track.mode = showSubtitles ? 'showing' : 'hidden';
      
      console.log('✅ DEBUG: Subtitle track added to video for language:', language);
      setDebugInfo(prev => ({
        ...prev,
        apiCalls: [...prev.apiCalls, `✅ Added subtitle track for ${language}`]
      }));
    } catch (error) {
      console.error('❌ DEBUG: Failed to add subtitle track:', error);
      setDebugInfo(prev => ({
        ...prev,
        errors: [...prev.errors, `Failed to add subtitle track: ${error.message}`]
      }));
    }
  };

  // Helper: Convert SRT to WebVTT format
  const convertSRTtoWebVTT = (srtContent) => {
    let vttContent = 'WEBVTT\n\n';
    
    // Replace SRT timestamp format (00:00:00,000) with WebVTT format (00:00:00.000)
    vttContent += srtContent.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2');
    
    return vttContent;
  };

  // Helper: Get language name
  const getLanguageName = (code) => {
    const languageMap = {
      'en': 'English',
      'hi': 'Hindi', 
      'te': 'Telugu'
    };
    return languageMap[code] || code.toUpperCase();
  };

  // Handle language change
  const handleLanguageChange = async (newLanguage) => {
    if (newLanguage === selectedLanguage) return;
    
    console.log('🌐 DEBUG: Changing subtitle language to:', newLanguage);
    setSelectedLanguage(newLanguage);
    const recordingId = recordingData.id || recordingData._id || recordingData.recordingId;
    await debugLoadSubtitles(recordingId, newLanguage);
  };

  // Toggle subtitles on/off
  const toggleSubtitles = () => {
    const newShowSubtitles = !showSubtitles;
    setShowSubtitles(newShowSubtitles);
    
    // Update video track mode
    if (trackRef.current) {
      trackRef.current.track.mode = newShowSubtitles ? 'showing' : 'hidden';
    }
    
    console.log('🎬 DEBUG: Subtitles toggled:', newShowSubtitles ? 'ON' : 'OFF');
  };

  // Download subtitles
  const handleDownloadSubtitles = async (format = 'srt', language = null) => {
    try {
      const downloadLang = language || selectedLanguage;
      const recordingId = recordingData.id || recordingData._id || recordingData.recordingId;
      const fileName = `${recordingData.title || recordingData.meeting_name || recordingData.filename || 'recording'}_subtitles_${downloadLang}.${format}`;
      await recordingsAPI.downloadSubtitles(recordingId, format, fileName, downloadLang);
      console.log('✅ DEBUG: Subtitles download initiated for language:', downloadLang);
    } catch (error) {
      console.error('❌ DEBUG: Failed to download subtitles:', error);
      alert('Failed to download subtitles: ' + error.message);
    }
  };

  // Video event handlers
  const handleLoadedMetadata = () => {
    if (videoRef.current) {
      setDuration(videoRef.current.duration);
      setIsLoading(false);
      setHasError(false);
      console.log('✅ DEBUG: Video loaded successfully, duration:', videoRef.current.duration);
    }
  };

  const handleTimeUpdate = () => {
    if (videoRef.current) {
      setCurrentTime(videoRef.current.currentTime);
    }
  };

  const handlePlay = () => setIsPlaying(true);
  const handlePause = () => setIsPlaying(false);

  const handleVideoError = (event) => {
    console.error('❌ DEBUG: Video error:', event);
    setHasError(true);
    setIsLoading(false);
    setDebugInfo(prev => ({
      ...prev,
      errors: [...prev.errors, `Video error: ${event.type}`]
    }));
  };

  const togglePlayPause = () => {
    if (videoRef.current) {
      if (isPlaying) {
        videoRef.current.pause();
      } else {
        videoRef.current.play().catch(err => {
          console.error('❌ DEBUG: Play failed:', err);
          setHasError(true);
        });
      }
    }
  };

  const handleSeek = (event, newValue) => {
    if (videoRef.current) {
      videoRef.current.currentTime = newValue;
      setCurrentTime(newValue);
    }
  };

  const handleVolumeChange = (event, newValue) => {
    setVolume(newValue);
    if (videoRef.current) {
      videoRef.current.volume = newValue;
    }
    setIsMuted(newValue === 0);
  };

  const toggleMute = () => {
    if (videoRef.current) {
      if (isMuted) {
        videoRef.current.volume = volume;
        setIsMuted(false);
      } else {
        videoRef.current.volume = 0;
        setIsMuted(true);
      }
    }
  };

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      containerRef.current?.requestFullscreen();
      setIsFullscreen(true);
    } else {
      document.exitFullscreen();
      setIsFullscreen(false);
    }
  };

  const skipTime = (seconds) => {
    if (videoRef.current) {
      const newTime = Math.max(0, Math.min(duration, videoRef.current.currentTime + seconds));
      videoRef.current.currentTime = newTime;
    }
  };

  const changeSpeed = (speed) => {
    if (videoRef.current) {
      videoRef.current.playbackRate = speed;
      setPlaybackSpeed(speed);
    }
  };

  // Get subtitle button state
  const getSubtitleButtonState = () => {
    if (subtitlesLoading) {
      return {
        icon: <CircularProgress size={20} sx={{ color: 'white' }} />,
        color: 'rgba(255,255,255,0.7)',
        tooltip: 'Loading subtitles...',
        disabled: true
      };
    }
    
    if (subtitlesAvailable) {
      return {
        icon: showSubtitles ? <SubtitlesIcon /> : <SubtitlesOffIcon />,
        color: showSubtitles ? theme.palette.primary.main : 'white',
        tooltip: showSubtitles ? 'Turn off subtitles' : 'Turn on subtitles',
        disabled: false,
        background: showSubtitles ? 'rgba(255,255,255,0.2)' : 'transparent'
      };
    }
    
    return {
      icon: <SubtitlesOffIcon />,
      color: 'rgba(255,255,255,0.5)',
      tooltip: 'No subtitles available',
      disabled: true
    };
  };

  const subtitleButtonState = getSubtitleButtonState();

  return (
    <Box sx={{ maxWidth: 1400, mx: 'auto', p: 2 }}>
      {/* DEBUG: Add debug button */}
      <Box sx={{ position: 'fixed', top: 10, right: 10, zIndex: 9999 }}>
        <IconButton
          onClick={() => setShowDebugDialog(true)}
          sx={{
            backgroundColor: 'red',
            color: 'white',
            '&:hover': { backgroundColor: 'darkred' }
          }}
        >
          <DebugIcon />
        </IconButton>
      </Box>

      {/* Header */}
      <Card sx={{ mb: 2, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={8}>
              <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold' }}>
                {recordingData.title || recordingData.meeting_name || recordingData.filename || 'Recording'}
              </Typography>
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap">
                <Chip 
                  label="HD Quality" 
                  sx={{ backgroundColor: 'rgba(255,255,255,0.2)', color: 'white' }}
                />
                <Chip 
                  label={formatTime(duration)} 
                  sx={{ backgroundColor: 'rgba(255,255,255,0.2)', color: 'white' }}
                  icon={<AccessTimeIcon sx={{ color: 'white !important' }} />}
                />
                {/* Dynamic subtitle status */}
                <Chip 
                  label={
                    subtitlesLoading ? 'Loading Subtitles...' :
                    subtitlesAvailable ? `Subtitles Available (${availableLanguages.length} languages)` : 
                    'No Subtitles Available'
                  } 
                  sx={{ backgroundColor: 'rgba(255,255,255,0.2)', color: 'white' }}
                  icon={
                    subtitlesLoading ? <CircularProgress size={16} sx={{ color: 'white !important' }} /> :
                    subtitlesAvailable ? <ClosedCaptionIcon sx={{ color: 'white !important' }} /> : 
                    <SubtitlesOffIcon sx={{ color: 'white !important' }} />
                  }
                />
              </Stack>
            </Grid>
            
            <Grid item xs={12} md={4}>
              <Stack direction="row" spacing={1} justifyContent="flex-end">
                {subtitlesAvailable && (
                  <Tooltip title="Download Subtitles">
                    <IconButton 
                      sx={{ color: 'white' }}
                      onClick={(e) => setSubtitleMenuAnchor(e.currentTarget)}
                    >
                      <GetAppIcon />
                    </IconButton>
                  </Tooltip>
                )}
                
                <Tooltip title="Share Recording">
                  <IconButton sx={{ color: 'white' }}>
                    <ShareIcon />
                  </IconButton>
                </Tooltip>
              </Stack>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      <Grid container spacing={2}>
        {/* Video Player */}
        <Grid item xs={12} lg={8}>
          <Card 
            ref={containerRef}
            sx={{ 
              position: 'relative', 
              backgroundColor: 'black',
              cursor: showControls ? 'default' : 'none'
            }}
            onMouseMove={resetControlsTimeout}
            onMouseLeave={() => isPlaying && setShowControls(false)}
          >
            <Box sx={{ position: 'relative', paddingTop: '56.25%' }}>
              {/* Video Element */}
              <video
                ref={videoRef}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: '100%',
                  objectFit: 'contain'
                }}
                src={actualVideoUrl}
                poster={actualThumbnailUrl}
                onLoadedMetadata={handleLoadedMetadata}
                onTimeUpdate={handleTimeUpdate}
                onPlay={handlePlay}
                onPause={handlePause}
                onError={handleVideoError}
                onClick={togglePlayPause}
                preload="metadata"
                crossOrigin="anonymous"
              >
                {/* Subtitle tracks will be added dynamically */}
              </video>
              
              {/* Error Display */}
              {hasError && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: '50%',
                    left: '50%',
                    transform: 'translate(-50%, -50%)',
                    color: 'white',
                    textAlign: 'center',
                    backgroundColor: 'rgba(0,0,0,0.8)',
                    p: 2,
                    borderRadius: 1
                  }}
                >
                  <Typography variant="h6" gutterBottom>
                    ❌ Video Error
                  </Typography>
                  <Typography variant="body2">
                    Unable to load video. Please check the video URL or try again later.
                  </Typography>
                </Box>
              )}
              
              {/* Loading Indicator */}
              {isLoading && !hasError && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: '50%',
                    left: '50%',
                    transform: 'translate(-50%, -50%)',
                    color: 'white',
                    textAlign: 'center'
                  }}
                >
                  <CircularProgress sx={{ color: 'white', mb: 2 }} />
                  <Typography>Loading video...</Typography>
                </Box>
              )}

              {/* Play Button Overlay */}
              {!isPlaying && !isLoading && !hasError && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: '50%',
                    left: '50%',
                    transform: 'translate(-50%, -50%)',
                    cursor: 'pointer'
                  }}
                  onClick={togglePlayPause}
                >
                  <IconButton
                    sx={{
                      backgroundColor: 'rgba(0,0,0,0.7)',
                      color: 'white',
                      width: 80,
                      height: 80,
                      '&:hover': {
                        backgroundColor: 'rgba(0,0,0,0.8)'
                      }
                    }}
                  >
                    <PlayIcon sx={{ fontSize: 40 }} />
                  </IconButton>
                </Box>
              )}

              {/* Video Controls */}
              <Box 
                sx={{ 
                  position: 'absolute',
                  bottom: 0,
                  left: 0,
                  right: 0,
                  background: 'linear-gradient(transparent, rgba(0,0,0,0.8))',
                  p: 2,
                  color: 'white',
                  opacity: showControls ? 1 : 0,
                  transition: 'opacity 0.3s ease'
                }}
              >
                {/* Progress Bar */}
                <Box sx={{ mb: 2 }}>
                  <Slider
                    value={currentTime}
                    max={duration}
                    onChange={handleSeek}
                    sx={{
                      color: theme.palette.primary.main,
                      height: 6,
                      '& .MuiSlider-thumb': {
                        width: 16,
                        height: 16,
                      },
                      '& .MuiSlider-track': {
                        border: 'none',
                      },
                      '& .MuiSlider-rail': {
                        backgroundColor: 'rgba(255,255,255,0.3)',
                      }
                    }}
                  />
                </Box>

                {/* Control Buttons */}
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Stack direction="row" alignItems="center" spacing={1}>
                    {/* Play/Pause */}
                    <IconButton onClick={togglePlayPause} sx={{ color: 'white' }}>
                      {isPlaying ? <PauseIcon /> : <PlayIcon />}
                    </IconButton>

                    {/* Skip Controls */}
                    <IconButton onClick={() => skipTime(-10)} sx={{ color: 'white' }}>
                      <Replay10Icon />
                    </IconButton>
                    <IconButton onClick={() => skipTime(10)} sx={{ color: 'white' }}>
                      <Forward10Icon />
                    </IconButton>

                    {/* Volume */}
                    <IconButton onClick={toggleMute} sx={{ color: 'white' }}>
                      {isMuted || volume === 0 ? <VolumeOffIcon /> : <VolumeIcon />}
                    </IconButton>
                    <Slider
                      value={isMuted ? 0 : volume}
                      onChange={handleVolumeChange}
                      min={0}
                      max={1}
                      step={0.1}
                      sx={{ 
                        width: 100, 
                        color: 'white',
                        '& .MuiSlider-track': {
                          border: 'none',
                        },
                        '& .MuiSlider-rail': {
                          backgroundColor: 'rgba(255,255,255,0.3)',
                        }
                      }}
                    />

                    {/* Time Display */}
                    <Typography variant="body2" sx={{ minWidth: 100, fontFamily: 'monospace' }}>
                      {formatTime(currentTime)} / {formatTime(duration)}
                    </Typography>
                  </Stack>

                  <Stack direction="row" alignItems="center" spacing={1}>
                    {/* Speed Control */}
                    <Button
                      onClick={() => {
                        const speeds = [0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2];
                        const currentIndex = speeds.indexOf(playbackSpeed);
                        const nextSpeed = speeds[(currentIndex + 1) % speeds.length];
                        changeSpeed(nextSpeed);
                      }}
                      sx={{ 
                        color: 'white', 
                        minWidth: 'auto',
                        backgroundColor: 'rgba(255,255,255,0.1)',
                        '&:hover': {
                          backgroundColor: 'rgba(255,255,255,0.2)'
                        }
                      }}
                    >
                      {playbackSpeed}x
                    </Button>

                    {/* Subtitle Button - YouTube Style */}
                    <Tooltip title={subtitleButtonState.tooltip}>
                      <span>
                        <IconButton 
                          onClick={subtitlesAvailable ? toggleSubtitles : undefined}
                          disabled={subtitleButtonState.disabled}
                          sx={{ 
                            color: subtitleButtonState.color,
                            backgroundColor: subtitleButtonState.background || 'transparent',
                            '&:hover': {
                              backgroundColor: 'rgba(255,255,255,0.2)'
                            },
                            '&:disabled': {
                              color: 'rgba(255,255,255,0.3)'
                            }
                          }}
                        >
                          {subtitleButtonState.icon}
                        </IconButton>
                      </span>
                    </Tooltip>

                    {/* Settings */}
                    <IconButton 
                      onClick={() => setShowSettings(true)}
                      sx={{ color: 'white' }}
                    >
                      <SettingsIcon />
                    </IconButton>

                    {/* Fullscreen */}
                    <IconButton onClick={toggleFullscreen} sx={{ color: 'white' }}>
                      {isFullscreen ? <FullscreenExitIcon /> : <FullscreenIcon />}
                    </IconButton>
                  </Stack>
                </Stack>
              </Box>
            </Box>
          </Card>
        </Grid>

        {/* Sidebar */}
        <Grid item xs={12} lg={4}>
          <Card sx={{ height: 600 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>Recording Information</Typography>
              <Divider sx={{ mb: 2 }} />
              
              <Stack spacing={2}>
                <Box>
                  <Typography variant="subtitle2" color="primary">Title</Typography>
                  <Typography variant="body2">
                    {recordingData.title || recordingData.meeting_name || recordingData.filename || 'Untitled Recording'}
                  </Typography>
                </Box>
                
                <Box>
                  <Typography variant="subtitle2" color="primary">Duration</Typography>
                  <Typography variant="body2">{formatTime(duration)}</Typography>
                </Box>
                
                <Box>
                  <Typography variant="subtitle2" color="primary">Quality</Typography>
                  <Typography variant="body2">{recordingData.quality || 'HD'}</Typography>
                </Box>
                
                <Box>
                  <Typography variant="subtitle2" color="primary">Subtitles</Typography>
                  <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
                    {subtitlesLoading ? (
                      <>
                        <CircularProgress size={16} />
                        <Typography variant="body2">Loading...</Typography>
                      </>
                    ) : subtitlesAvailable ? (
                      <>
                        <Typography variant="body2">Available</Typography>
                        {showSubtitles && <Chip label="ON" size="small" color="primary" />}
                        {!showSubtitles && <Chip label="OFF" size="small" variant="outlined" />}
                        
                        {/* Language Selection */}
                        {availableLanguages.length > 1 && (
                          <FormControl size="small" sx={{ minWidth: 120, mt: 1 }}>
                            <InputLabel>Language</InputLabel>
                            <Select
                              value={selectedLanguage}
                              label="Language"
                              onChange={(e) => handleLanguageChange(e.target.value)}
                            >
                              {availableLanguages.map((lang) => (
                                <MenuItem key={lang.code} value={lang.code}>
                                  {lang.name}
                                </MenuItem>
                              ))}
                            </Select>
                          </FormControl>
                        )}
                      </>
                    ) : (
                      <Typography variant="body2">Not Available</Typography>
                    )}
                  </Stack>
                </Box>

                {/* Subtitle Download Options */}
                {subtitlesAvailable && (
                  <Box>
                    <Typography variant="subtitle2" color="primary" gutterBottom>
                      Download Subtitles
                    </Typography>
                    <Stack spacing={1}>
                      <Button
                        variant="outlined"
                        size="small"
                        onClick={() => handleDownloadSubtitles('srt', selectedLanguage)}
                        startIcon={<DownloadIcon />}
                      >
                        Download SRT ({getLanguageName(selectedLanguage)})
                      </Button>
                      
                      {availableLanguages.length > 1 && (
                        <Button
                          variant="outlined"
                          size="small"
                          onClick={() => setSubtitleMenuAnchor(document.querySelector('#subtitle-download-btn'))}
                          startIcon={<LanguageIcon />}
                          id="subtitle-download-btn"
                        >
                          All Languages
                        </Button>
                      )}
                    </Stack>
                  </Box>
                )}

                {/* Show available languages */}
                {availableLanguages.length > 0 && (
                  <Box>
                    <Typography variant="subtitle2" color="primary" gutterBottom>
                      Available Languages
                    </Typography>
                    <Stack direction="row" spacing={1} flexWrap="wrap">
                      {availableLanguages.map((lang) => (
                        <Chip
                          key={lang.code}
                          label={lang.name}
                          size="small"
                          color={lang.code === selectedLanguage ? "primary" : "default"}
                          onClick={() => handleLanguageChange(lang.code)}
                          style={{ cursor: 'pointer' }}
                        />
                      ))}
                    </Stack>
                  </Box>
                )}
              </Stack>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* DEBUG DIALOG */}
      <Dialog 
        open={showDebugDialog} 
        onClose={() => setShowDebugDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>🐛 Debug Information</DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            <Alert severity="info">
              This debug dialog shows detailed information about subtitle loading process. 
              Share this info if you need help debugging.
            </Alert>
            
            <Box>
              <Typography variant="h6" gutterBottom>Recording Data</Typography>
              <Typography variant="body2" component="pre" sx={{ 
                backgroundColor: '#f5f5f5', 
                p: 1, 
                borderRadius: 1,
                fontSize: '0.8em',
                overflow: 'auto',
                maxHeight: 200
              }}>
                {JSON.stringify({
                  recordingId: debugInfo.recordingId,
                  recordingDataKeys: debugInfo.recordingDataKeys,
                  hasId: !!recordingData.id,
                  hasMongoId: !!recordingData._id,
                  hasRecordingId: !!recordingData.recordingId,
                  subtitlesField: recordingData.subtitles,
                  actualRecordingData: recordingData
                }, null, 2)}
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="h6" gutterBottom>API Calls</Typography>
              <Stack spacing={1}>
                {debugInfo.apiCalls.map((call, index) => (
                  <Typography key={index} variant="body2" sx={{ 
                    fontFamily: 'monospace',
                    backgroundColor: call.includes('✅') ? '#e8f5e8' : '#f5f5f5',
                    p: 0.5,
                    borderRadius: 1
                  }}>
                    {call}
                  </Typography>
                ))}
              </Stack>
            </Box>
            
            <Box>
              <Typography variant="h6" gutterBottom>Errors</Typography>
              {debugInfo.errors.length === 0 ? (
                <Typography variant="body2" color="success.main">No errors found</Typography>
              ) : (
                <Stack spacing={1}>
                  {debugInfo.errors.map((error, index) => (
                    <Alert key={index} severity="error" sx={{ fontSize: '0.8em' }}>
                      {error}
                    </Alert>
                  ))}
                </Stack>
              )}
            </Box>

            <Box>
              <Typography variant="h6" gutterBottom>Full Recording Response</Typography>
              {debugInfo.fullRecordingData && (
                <Typography variant="body2" component="pre" sx={{ 
                  backgroundColor: '#f5f5f5', 
                  p: 1, 
                  borderRadius: 1,
                  fontSize: '0.8em',
                  overflow: 'auto',
                  maxHeight: 300
                }}>
                  {JSON.stringify(debugInfo.fullRecordingData, null, 2)}
                </Typography>
              )}
            </Box>

            <Box>
              <Typography variant="h6" gutterBottom>Available Languages</Typography>
              <Typography variant="body2" component="pre" sx={{ 
                backgroundColor: '#f5f5f5', 
                p: 1, 
                borderRadius: 1,
                fontSize: '0.8em'
              }}>
                {JSON.stringify(debugInfo.availableLanguages || [], null, 2)}
              </Typography>
            </Box>

            {debugInfo.subtitleContent && (
              <Box>
                <Typography variant="h6" gutterBottom>Subtitle Content Preview</Typography>
                <Typography variant="body2" component="pre" sx={{ 
                  backgroundColor: '#f5f5f5', 
                  p: 1, 
                  borderRadius: 1,
                  fontSize: '0.8em',
                  overflow: 'auto',
                  maxHeight: 150
                }}>
                  {debugInfo.subtitleContent}
                </Typography>
              </Box>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            const debugData = JSON.stringify(debugInfo, null, 2);
            navigator.clipboard.writeText(debugData);
            alert('Debug info copied to clipboard!');
          }}>
            Copy Debug Info
          </Button>
          <Button onClick={() => setShowDebugDialog(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Subtitle Download Menu for Multiple Languages */}
      <Menu
        anchorEl={subtitleMenuAnchor}
        open={Boolean(subtitleMenuAnchor)}
        onClose={() => setSubtitleMenuAnchor(null)}
      >
        {availableLanguages.map((lang) => (
          <MenuItem key={lang.code} onClick={() => {
            handleDownloadSubtitles('srt', lang.code);
            setSubtitleMenuAnchor(null);
          }}>
            <ListItemIcon><GetAppIcon fontSize="small" /></ListItemIcon>
            Download {lang.name} SRT
          </MenuItem>
        ))}
      </Menu>

      {/* Settings Dialog */}
      <Dialog 
        open={showSettings} 
        onClose={() => setShowSettings(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Player Settings</DialogTitle>
        <DialogContent>
          <Stack spacing={3}>
            {/* Subtitle Settings */}
            <Box>
              <Typography variant="h6" gutterBottom>Subtitle Settings</Typography>
              
              {subtitlesAvailable ? (
                <Stack spacing={2}>
                  <Alert severity="success">
                    Subtitles are available for this recording in {availableLanguages.length} language(s).
                  </Alert>
                  
                  <Typography variant="body2">
                    Status: <strong>Available</strong> • Currently: <strong>{showSubtitles ? 'ON' : 'OFF'}</strong>
                  </Typography>
                  
                  {/* Language Selection */}
                  {availableLanguages.length > 1 && (
                    <FormControl fullWidth>
                      <InputLabel>Subtitle Language</InputLabel>
                      <Select
                        value={selectedLanguage}
                        label="Subtitle Language"
                        onChange={(e) => handleLanguageChange(e.target.value)}
                      >
                        {availableLanguages.map((lang) => (
                          <MenuItem key={lang.code} value={lang.code}>
                            {lang.name}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  )}
                  
                  <Button
                    variant={showSubtitles ? "contained" : "outlined"}
                    onClick={toggleSubtitles}
                    startIcon={showSubtitles ? <SubtitlesIcon /> : <SubtitlesOffIcon />}
                  >
                    {showSubtitles ? 'Turn Off Subtitles' : 'Turn On Subtitles'}
                  </Button>
                </Stack>
              ) : (
                <Alert severity="info">
                  No subtitles are available for this recording.
                </Alert>
              )}
            </Box>
            
            <Divider />
            
            <Box>
              <Typography gutterBottom>Playback Speed</Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap">
                {[0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2].map((speed) => (
                  <Button
                    key={speed}
                    variant={playbackSpeed === speed ? "contained" : "outlined"}
                    onClick={() => changeSpeed(speed)}
                    size="small"
                    sx={{ mb: 1 }}
                  >
                    {speed}x
                  </Button>
                ))}
              </Stack>
            </Box>

            <Alert severity="info">
              Use keyboard shortcuts: Space (play/pause), ← → (seek), ↑ ↓ (volume), F (fullscreen)
            </Alert>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowSettings(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RecordingPlayer;