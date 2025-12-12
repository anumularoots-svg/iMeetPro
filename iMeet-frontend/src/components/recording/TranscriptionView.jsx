import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Chip,
  IconButton,
  TextField,
  InputAdornment,
  List,
  ListItem,
  ListItemText,
  Divider,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
  Alert,
  Tooltip,
  Stack,
  Paper,
  Avatar
} from '@mui/material';
import {
  Search,
  Download,
  PlayArrow,
  Pause,
  VolumeUp,
  Translate,
  Share,
  Edit,
  Bookmark,
  Close,
  ContentCopy,
  FilterList
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const TranscriptionView = ({ meetingId, recordingId, isOpen, onClose }) => {
  const theme = useTheme();
  const [transcription, setTranscription] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedSpeaker, setSelectedSpeaker] = useState('all');
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [speakers, setSpeakers] = useState([]);
  const [showDownloadDialog, setShowDownloadDialog] = useState(false);
  const [downloadFormat, setDownloadFormat] = useState('txt');

  // Mock transcription data - replace with API call
  useEffect(() => {
    const fetchTranscription = async () => {
      setLoading(true);
      try {
        // Replace with actual API call
        const mockData = [
          {
            id: 1,
            speaker: 'John Doe',
            text: 'Welcome everyone to today\'s meeting. Let\'s start by reviewing the agenda.',
            timestamp: '00:00:15',
            confidence: 0.95,
            keywords: ['welcome', 'meeting', 'agenda']
          },
          {
            id: 2,
            speaker: 'Sarah Smith',
            text: 'Thank you John. I\'d like to discuss the quarterly results first.',
            timestamp: '00:00:28',
            confidence: 0.92,
            keywords: ['quarterly', 'results']
          },
          {
            id: 3,
            speaker: 'Mike Johnson',
            text: 'The numbers look promising. Revenue is up 15% from last quarter.',
            timestamp: '00:00:45',
            confidence: 0.88,
            keywords: ['revenue', '15%', 'quarter']
          }
        ];
        
        setTranscription(mockData);
        setSpeakers(['John Doe', 'Sarah Smith', 'Mike Johnson']);
      } catch (error) {
        console.error('Error fetching transcription:', error);
      } finally {
        setLoading(false);
      }
    };

    if (isOpen && recordingId) {
      fetchTranscription();
    }
  }, [isOpen, recordingId]);

  const filteredTranscription = transcription.filter(item => {
    const matchesSearch = item.text.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesSpeaker = selectedSpeaker === 'all' || item.speaker === selectedSpeaker;
    return matchesSearch && matchesSpeaker;
  });

  const handleJumpToTime = (timestamp) => {
    // Logic to jump to specific time in recording
    console.log(`Jumping to ${timestamp}`);
  };

  const handleDownload = () => {
    const content = transcription.map(item => 
      `[${item.timestamp}] ${item.speaker}: ${item.text}`
    ).join('\n');
    
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `transcription_${meetingId}.${downloadFormat}`;
    a.click();
    URL.revokeObjectURL(url);
    setShowDownloadDialog(false);
  };

  const handleCopyToClipboard = () => {
    const content = filteredTranscription.map(item => 
      `[${item.timestamp}] ${item.speaker}: ${item.text}`
    ).join('\n');
    navigator.clipboard.writeText(content);
  };

  const getConfidenceColor = (confidence) => {
    if (confidence >= 0.9) return 'success';
    if (confidence >= 0.7) return 'warning';
    return 'error';
  };

  const getSpeakerAvatar = (speakerName) => {
    const colors = ['#1976d2', '#388e3c', '#f57c00', '#d32f2f', '#7b1fa2'];
    const index = speakers.indexOf(speakerName) % colors.length;
    return {
      bgcolor: colors[index],
      color: 'white'
    };
  };

  return (
    <Dialog
      open={isOpen}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          height: '80vh',
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white'
        }
      }}
    >
      <DialogTitle sx={{ 
        background: 'rgba(255,255,255,0.1)', 
        backdropFilter: 'blur(10px)',
        borderBottom: '1px solid rgba(255,255,255,0.2)'
      }}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h5" component="div" sx={{ fontWeight: 600 }}>
            Meeting Transcription
          </Typography>
          <IconButton onClick={onClose} sx={{ color: 'white' }}>
            <Close />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ p: 0, height: '100%', overflow: 'hidden' }}>
        <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
          {/* Search and Filter Bar */}
          <Paper sx={{ 
            m: 2, 
            p: 2, 
            background: 'rgba(255,255,255,0.1)', 
            backdropFilter: 'blur(10px)',
            border: '1px solid rgba(255,255,255,0.2)'
          }}>
            <Stack direction="row" spacing={2} alignItems="center">
              <TextField
                placeholder="Search transcription..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                variant="outlined"
                size="small"
                sx={{ 
                  flex: 1,
                  '& .MuiOutlinedInput-root': {
                    color: 'white',
                    '& fieldset': { borderColor: 'rgba(255,255,255,0.3)' },
                    '&:hover fieldset': { borderColor: 'rgba(255,255,255,0.5)' },
                    '&.Mui-focused fieldset': { borderColor: 'white' }
                  },
                  '& .MuiInputBase-input::placeholder': { color: 'rgba(255,255,255,0.7)' }
                }}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Search sx={{ color: 'rgba(255,255,255,0.7)' }} />
                    </InputAdornment>
                  )
                }}
              />
              
              <TextField
                select
                label="Speaker"
                value={selectedSpeaker}
                onChange={(e) => setSelectedSpeaker(e.target.value)}
                variant="outlined"
                size="small"
                sx={{ 
                  minWidth: 150,
                  '& .MuiOutlinedInput-root': {
                    color: 'white',
                    '& fieldset': { borderColor: 'rgba(255,255,255,0.3)' }
                  },
                  '& .MuiInputLabel-root': { color: 'rgba(255,255,255,0.7)' }
                }}
                SelectProps={{
                  native: true
                }}
              >
                <option value="all">All Speakers</option>
                {speakers.map(speaker => (
                  <option key={speaker} value={speaker}>{speaker}</option>
                ))}
              </TextField>

              <Tooltip title="Download Transcription">
                <IconButton 
                  onClick={() => setShowDownloadDialog(true)}
                  sx={{ color: 'white' }}
                >
                  <Download />
                </IconButton>
              </Tooltip>

              <Tooltip title="Copy to Clipboard">
                <IconButton 
                  onClick={handleCopyToClipboard}
                  sx={{ color: 'white' }}
                >
                  <ContentCopy />
                </IconButton>
              </Tooltip>

              <Tooltip title="Share">
                <IconButton sx={{ color: 'white' }}>
                  <Share />
                </IconButton>
              </Tooltip>
            </Stack>
          </Paper>

          {/* Transcription Content */}
          <Box sx={{ flex: 1, overflow: 'auto', px: 2 }}>
            {loading ? (
              <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                <CircularProgress sx={{ color: 'white' }} />
                <Typography sx={{ ml: 2 }}>Loading transcription...</Typography>
              </Box>
            ) : filteredTranscription.length === 0 ? (
              <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                <Typography variant="h6">No transcription data found</Typography>
              </Box>
            ) : (
              <List sx={{ pb: 2 }}>
                {filteredTranscription.map((item, index) => (
                  <React.Fragment key={item.id}>
                    <ListItem
                      sx={{
                        mb: 1,
                        background: 'rgba(255,255,255,0.05)',
                        borderRadius: 2,
                        border: '1px solid rgba(255,255,255,0.1)',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          background: 'rgba(255,255,255,0.1)',
                          transform: 'translateY(-2px)',
                          boxShadow: '0 4px 20px rgba(0,0,0,0.3)'
                        }
                      }}
                    >
                      <Box sx={{ display: 'flex', width: '100%', gap: 2 }}>
                        <Avatar sx={getSpeakerAvatar(item.speaker)}>
                          {item.speaker.charAt(0)}
                        </Avatar>
                        
                        <Box sx={{ flex: 1 }}>
                          <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                              {item.speaker}
                            </Typography>
                            <Stack direction="row" spacing={1} alignItems="center">
                              <Chip
                                label={item.timestamp}
                                size="small"
                                clickable
                                onClick={() => handleJumpToTime(item.timestamp)}
                                sx={{
                                  bgcolor: 'rgba(255,255,255,0.2)',
                                  color: 'white',
                                  '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
                                }}
                                icon={<PlayArrow sx={{ color: 'white !important' }} />}
                              />
                              <Chip
                                label={`${Math.round(item.confidence * 100)}%`}
                                size="small"
                                color={getConfidenceColor(item.confidence)}
                                variant="outlined"
                              />
                            </Stack>
                          </Box>
                          
                          <Typography variant="body1" sx={{ lineHeight: 1.6, mb: 1 }}>
                            {item.text}
                          </Typography>
                          
                          {item.keywords && (
                            <Stack direction="row" spacing={1} flexWrap="wrap">
                              {item.keywords.map((keyword, idx) => (
                                <Chip
                                  key={idx}
                                  label={keyword}
                                  size="small"
                                  variant="outlined"
                                  sx={{
                                    borderColor: 'rgba(255,255,255,0.3)',
                                    color: 'rgba(255,255,255,0.8)',
                                    fontSize: '0.75rem'
                                  }}
                                />
                              ))}
                            </Stack>
                          )}
                        </Box>

                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                          <Tooltip title="Bookmark">
                            <IconButton size="small" sx={{ color: 'white' }}>
                              <Bookmark />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Edit">
                            <IconButton size="small" sx={{ color: 'white' }}>
                              <Edit />
                            </IconButton>
                          </Tooltip>
                        </Box>
                      </Box>
                    </ListItem>
                    {index < filteredTranscription.length - 1 && (
                      <Divider sx={{ bgcolor: 'rgba(255,255,255,0.1)', my: 1 }} />
                    )}
                  </React.Fragment>
                ))}
              </List>
            )}
          </Box>
        </Box>
      </DialogContent>

      {/* Download Dialog */}
      <Dialog open={showDownloadDialog} onClose={() => setShowDownloadDialog(false)}>
        <DialogTitle>Download Transcription</DialogTitle>
        <DialogContent>
          <TextField
            select
            label="Format"
            value={downloadFormat}
            onChange={(e) => setDownloadFormat(e.target.value)}
            fullWidth
            margin="dense"
            SelectProps={{ native: true }}
          >
            <option value="txt">Text (.txt)</option>
            <option value="pdf">PDF (.pdf)</option>
            <option value="docx">Word (.docx)</option>
            <option value="json">JSON (.json)</option>
          </TextField>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowDownloadDialog(false)}>Cancel</Button>
          <Button onClick={handleDownload} variant="contained">Download</Button>
        </DialogActions>
      </Dialog>
    </Dialog>
  );
};

export default TranscriptionView;