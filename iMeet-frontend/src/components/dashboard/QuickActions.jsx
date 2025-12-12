// import React, { useState } from 'react';
// import {
//   Box,
//   Card,
//   CardContent,
//   Typography,
//   Button,
//   IconButton,
//   TextField,
//   Dialog,
//   DialogTitle,
//   DialogContent,
//   DialogActions,
//   Chip,
//   Grid,
//   Tooltip,
//   useTheme,
//   alpha,
//   Snackbar,
//   Alert
// } from '@mui/material';
// import {
//   VideoCall,
//   Launch,
//   ContentCopy,
//   Schedule,
//   CalendarMonth,
//   Link,
//   QrCode,
//   Share,
//   Close,
//   CheckCircle
// } from '@mui/icons-material';
// import { useNavigate } from 'react-router-dom';

// const QuickActions = () => {
//   const theme = useTheme();
//   const navigate = useNavigate();
//   const [joinDialog, setJoinDialog] = useState(false);
//   const [shareDialog, setShareDialog] = useState(false);
//   const [meetingId, setMeetingId] = useState('');
//   const [generatedLink, setGeneratedLink] = useState('');
//   const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' });

//   // FIXED: Direct navigation to instant meeting like "Start Now" button
//   const handleInstantMeeting = () => {
//     console.log('🚀 Quick Actions: Navigating to instant meeting...');
//     navigate('/meeting/instant');
//   };

//   const quickActions = [
//     {
//       id: 'instant',
//       title: 'Start Instant Meeting',
//       description: 'Create and start a meeting now',
//       icon: <VideoCall />,
//       color: '#4CAF50',
//       gradient: 'linear-gradient(135deg, #4CAF50 0%, #45a049 100%)',
//       action: handleInstantMeeting // Now uses direct navigation
//     },
//     {
//       id: 'join',
//       title: 'Join Meeting',
//       description: 'Enter meeting ID or link',
//       icon: <Launch />,
//       color: '#2196F3',
//       gradient: 'linear-gradient(135deg, #2196F3 0%, #1976D2 100%)',
//       action: () => setJoinDialog(true)
//     },
//     {
//       id: 'schedule',
//       title: 'Schedule Meeting',
//       description: 'Plan for later',
//       icon: <Schedule />,
//       color: '#FF9800',
//       gradient: 'linear-gradient(135deg, #FF9800 0%, #F57C00 100%)',
//       action: () => navigate('/schedule')
//     },
//     {
//       id: 'calendar',
//       title: 'Calendar',
//       description: 'View scheduled meetings',
//       icon: <CalendarMonth />,
//       color: '#9C27B0',
//       gradient: 'linear-gradient(135deg, #9C27B0 0%, #7B1FA2 100%)',
//       action: () => navigate('/calendar')
//     }
//   ];

//   const handleJoinMeeting = () => {
//     if (!meetingId.trim()) {
//       setSnackbar({
//         open: true,
//         message: 'Please enter a meeting ID or link',
//         severity: 'error'
//       });
//       return;
//     }

//     // Extract meeting ID from URL if full link is provided
//     let id = meetingId;
//     if (meetingId.includes('/meeting/')) {
//       id = meetingId.split('/meeting/')[1];
//     }

//     navigate(`/meeting/${id}`);
//     setJoinDialog(false);
//     setMeetingId('');
//   };

//   const handleCopyLink = () => {
//     navigator.clipboard.writeText(generatedLink);
//     setSnackbar({
//       open: true,
//       message: 'Meeting link copied to clipboard!',
//       severity: 'success'
//     });
//   };

//   const handleShareLink = () => {
//     if (navigator.share) {
//       navigator.share({
//         title: 'Join my meeting',
//         text: 'Click the link to join my video meeting',
//         url: generatedLink
//       });
//     } else {
//       handleCopyLink();
//     }
//   };

//   return (
//     <Box sx={{ p: 3 }}>
//       {/* Header */}
//       <Box sx={{ mb: 3, textAlign: 'center' }}>
//         <Typography 
//           variant="h5" 
//           sx={{ 
//             fontWeight: 700,
//             mb: 1,
//             background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
//             backgroundClip: 'text',
//             WebkitBackgroundClip: 'text',
//             WebkitTextFillColor: 'transparent'
//           }}
//         >
//           Quick Actions
//         </Typography>
//         <Typography variant="body2" color="text.secondary">
//           Start or join meetings with one click
//         </Typography>
//       </Box>

//       {/* Quick Action Cards */}
//       <Grid container spacing={2}>
//         {quickActions.map((action) => (
//           <Grid item xs={12} sm={6} md={3} key={action.id}>
//             <Card
//               sx={{
//                 cursor: 'pointer',
//                 transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
//                 border: '1px solid',
//                 borderColor: 'divider',
//                 background: `linear-gradient(135deg, ${alpha(action.color, 0.05)} 0%, ${alpha(action.color, 0.02)} 100%)`,
//                 '&:hover': {
//                   transform: 'translateY(-4px)',
//                   boxShadow: `0 12px 24px ${alpha(action.color, 0.25)}`,
//                   borderColor: action.color,
//                   '& .action-icon': {
//                     transform: 'scale(1.1)',
//                     background: action.gradient,
//                     color: 'white',
//                   }
//                 }
//               }}
//               onClick={action.action}
//             >
//               <CardContent sx={{ textAlign: 'center', py: 3 }}>
//                 <Box
//                   className="action-icon"
//                   sx={{
//                     display: 'inline-flex',
//                     alignItems: 'center',
//                     justifyContent: 'center',
//                     width: 56,
//                     height: 56,
//                     borderRadius: '50%',
//                     backgroundColor: alpha(action.color, 0.1),
//                     color: action.color,
//                     mb: 2,
//                     transition: 'all 0.3s ease',
//                     fontSize: 24
//                   }}
//                 >
//                   {action.icon}
//                 </Box>
//                 <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
//                   {action.title}
//                 </Typography>
//                 <Typography variant="body2" color="text.secondary">
//                   {action.description}
//                 </Typography>
//               </CardContent>
//             </Card>
//           </Grid>
//         ))}
//       </Grid>

//       {/* Recent Activity */}
//       <Card sx={{ mt: 4, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)' }}>
//         <CardContent sx={{ p: 3, color: 'white' }}>
//           <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
//             Meeting Tips
//           </Typography>
//           <Grid container spacing={2}>
//             <Grid item xs={12} md={6}>
//               <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
//                 <CheckCircle sx={{ mr: 1, fontSize: 20 }} />
//                 <Typography variant="body2">Test your camera and mic before important meetings</Typography>
//               </Box>
//               <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
//                 <CheckCircle sx={{ mr: 1, fontSize: 20 }} />
//                 <Typography variant="body2">Use waiting rooms for secure meetings</Typography>
//               </Box>
//             </Grid>
//             <Grid item xs={12} md={6}>
//               <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
//                 <CheckCircle sx={{ mr: 1, fontSize: 20 }} />
//                 <Typography variant="body2">Enable recording for important discussions</Typography>
//               </Box>
//               <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
//                 <CheckCircle sx={{ mr: 1, fontSize: 20 }} />
//                 <Typography variant="body2">Share your screen for better collaboration</Typography>
//               </Box>
//             </Grid>
//           </Grid>
//         </CardContent>
//       </Card>

//       {/* Join Meeting Dialog */}
//       <Dialog open={joinDialog} onClose={() => setJoinDialog(false)} maxWidth="sm" fullWidth>
//         <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
//           <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
//             <Launch color="primary" />
//             <Typography variant="h6">Join Meeting</Typography>
//           </Box>
//           <IconButton onClick={() => setJoinDialog(false)} size="small">
//             <Close />
//           </IconButton>
//         </DialogTitle>
//         <DialogContent>
//           <TextField
//             autoFocus
//             fullWidth
//             label="Meeting ID or Link"
//             placeholder="Enter meeting ID or paste meeting link"
//             value={meetingId}
//             onChange={(e) => setMeetingId(e.target.value)}
//             variant="outlined"
//             sx={{ mt: 2 }}
//             InputProps={{
//               startAdornment: <Link sx={{ mr: 1, color: 'action.active' }} />
//             }}
//           />
//           <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
//             You can enter either a meeting ID (e.g., 123-456-789) or paste a full meeting link
//           </Typography>
//         </DialogContent>
//         <DialogActions sx={{ p: 3, pt: 1 }}>
//           <Button onClick={() => setJoinDialog(false)}>Cancel</Button>
//           <Button 
//             variant="contained" 
//             onClick={handleJoinMeeting}
//             disabled={!meetingId.trim()}
//           >
//             Join Meeting
//           </Button>
//         </DialogActions>
//       </Dialog>

//       {/* Share Meeting Dialog */}
//       <Dialog open={shareDialog} onClose={() => setShareDialog(false)} maxWidth="sm" fullWidth>
//         <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
//           <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
//             <VideoCall color="success" />
//             <Typography variant="h6">Meeting Created!</Typography>
//           </Box>
//           <IconButton onClick={() => setShareDialog(false)} size="small">
//             <Close />
//           </IconButton>
//         </DialogTitle>
//         <DialogContent>
//           <Typography variant="body1" sx={{ mb: 2 }}>
//             Your meeting is ready. Share this link with participants:
//           </Typography>
          
//           <TextField
//             fullWidth
//             value={generatedLink}
//             variant="outlined"
//             InputProps={{
//               readOnly: true,
//               endAdornment: (
//                 <Tooltip title="Copy Link">
//                   <IconButton onClick={handleCopyLink}>
//                     <ContentCopy />
//                   </IconButton>
//                 </Tooltip>
//               )
//             }}
//             sx={{ mb: 2 }}
//           />

//           <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
//             <Chip
//               icon={<ContentCopy />}
//               label="Copy Link"
//               onClick={handleCopyLink}
//               clickable
//               color="primary"
//               variant="outlined"
//             />
//             <Chip
//               icon={<Share />}
//               label="Share"
//               onClick={handleShareLink}
//               clickable
//               color="primary"
//               variant="outlined"
//             />
//             <Chip
//               icon={<QrCode />}
//               label="QR Code"
//               clickable
//               color="primary"
//               variant="outlined"
//             />
//           </Box>
//         </DialogContent>
//         <DialogActions sx={{ p: 3, pt: 1 }}>
//           <Button onClick={() => setShareDialog(false)}>Close</Button>
//           <Button 
//             variant="contained" 
//             onClick={() => {
//               navigate(`/meeting/${generatedLink.split('/meeting/')[1]}`);
//               setShareDialog(false);
//             }}
//           >
//             Start Meeting
//           </Button>
//         </DialogActions>
//       </Dialog>

//       {/* Snackbar for notifications */}
//       <Snackbar
//         open={snackbar.open}
//         autoHideDuration={3000}
//         onClose={() => setSnackbar({ ...snackbar, open: false })}
//       >
//         <Alert 
//           severity={snackbar.severity} 
//           onClose={() => setSnackbar({ ...snackbar, open: false })}
//         >
//           {snackbar.message}
//         </Alert>
//       </Snackbar>
//     </Box>
//   );
// };

// export default QuickActions;





import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  IconButton,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
  Grid,
  Tooltip,
  useTheme,
  alpha,
  Snackbar,
  Alert,
  Stack,
  Paper,
  Divider,
  InputAdornment
} from '@mui/material';
import {
  VideoCall,
  Launch,
  ContentCopy,
  Schedule,
  CalendarMonth,
  Link,
  QrCode,
  Share,
  Close,
  CheckCircle,
  ArrowForward
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

const QuickActions = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const [joinDialog, setJoinDialog] = useState(false);
  const [shareDialog, setShareDialog] = useState(false);
  const [meetingId, setMeetingId] = useState('');
  const [generatedLink, setGeneratedLink] = useState('');
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' });

  // FIXED: Direct navigation to instant meeting like "Start Now" button
  const handleInstantMeeting = () => {
    console.log('🚀 Quick Actions: Navigating to instant meeting...');
    navigate('/meeting/instant');
  };

  const quickActions = [
    {
      id: 'instant',
      title: 'Start Meeting',
      description: 'Create and start immediately',
      icon: <VideoCall />,
      color: '#4CAF50',
      action: handleInstantMeeting,
      primary: true
    },
    {
      id: 'join',
      title: 'Join Meeting',
      description: 'Enter meeting ID or link',
      icon: <Launch />,
      color: '#2196F3',
      action: () => setJoinDialog(true)
    },
    {
      id: 'schedule',
      title: 'Schedule',
      description: 'Plan for later',
      icon: <Schedule />,
      color: '#FF9800',
      action: () => navigate('/schedule')
    },
    {
      id: 'calendar',
      title: 'Calendar',
      description: 'View appointments',
      icon: <CalendarMonth />,
      color: '#9C27B0',
      action: () => navigate('/calendar')
    }
  ];

  const handleJoinMeeting = () => {
    if (!meetingId.trim()) {
      setSnackbar({
        open: true,
        message: 'Please enter a meeting ID or link',
        severity: 'error'
      });
      return;
    }

    // Extract meeting ID from URL if full link is provided
    let id = meetingId;
    if (meetingId.includes('/meeting/')) {
      id = meetingId.split('/meeting/')[1];
    }

    navigate(`/meeting/${id}`);
    setJoinDialog(false);
    setMeetingId('');
  };

  const handleCopyLink = () => {
    navigator.clipboard.writeText(generatedLink);
    setSnackbar({
      open: true,
      message: 'Meeting link copied to clipboard!',
      severity: 'success'
    });
  };

  const handleShareLink = () => {
    if (navigator.share) {
      navigator.share({
        title: 'Join my meeting',
        text: 'Click the link to join my video meeting',
        url: generatedLink
      });
    } else {
      handleCopyLink();
    }
  };

  return (
    <Box>
      {/* Primary Actions */}
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {/* Start Meeting - Primary Action */}
        <Grid item xs={12} md={6}>
          <Paper
            elevation={0}
            sx={{
              p: 3,
              border: '2px solid',
              borderColor: theme.palette.success.main,
              backgroundColor: `${theme.palette.success.main}08`,
              borderRadius: 2,
              cursor: 'pointer',
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: `${theme.palette.success.main}15`,
                transform: 'translateY(-2px)',
                boxShadow: `0 8px 25px ${theme.palette.success.main}20`
              }
            }}
            onClick={handleInstantMeeting}
          >
            <Stack direction="row" spacing={2} alignItems="center">
              <Box
                sx={{
                  width: 48,
                  height: 48,
                  borderRadius: 1.5,
                  backgroundColor: theme.palette.success.main,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'white'
                }}
              >
                <VideoCall sx={{ fontSize: 24 }} />
              </Box>
              <Box sx={{ flex: 1 }}>
                <Typography variant="h6" fontWeight={600} color="text.primary">
                  Start Meeting
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Create and start immediately
                </Typography>
              </Box>
              <ArrowForward sx={{ color: theme.palette.success.main }} />
            </Stack>
          </Paper>
        </Grid>

        {/* Join Meeting - Secondary Action */}
        <Grid item xs={12} md={6}>
          <Paper
            elevation={0}
            sx={{
              p: 3,
              border: '1px solid',
              borderColor: theme.palette.divider,
              borderRadius: 2,
              cursor: 'pointer',
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                borderColor: theme.palette.primary.main,
                backgroundColor: `${theme.palette.primary.main}05`,
                transform: 'translateY(-2px)',
                boxShadow: `0 8px 25px ${theme.palette.primary.main}15`
              }
            }}
            onClick={() => setJoinDialog(true)}
          >
            <Stack direction="row" spacing={2} alignItems="center">
              <Box
                sx={{
                  width: 48,
                  height: 48,
                  borderRadius: 1.5,
                  backgroundColor: `${theme.palette.primary.main}15`,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: theme.palette.primary.main
                }}
              >
                <Launch sx={{ fontSize: 24 }} />
              </Box>
              <Box sx={{ flex: 1 }}>
                <Typography variant="h6" fontWeight={600} color="text.primary">
                  Join Meeting
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Enter meeting ID or link
                </Typography>
              </Box>
              <ArrowForward sx={{ color: theme.palette.text.secondary }} />
            </Stack>
          </Paper>
        </Grid>
      </Grid>

      {/* Secondary Actions */}
      <Grid container spacing={2}>
        <Grid item xs={12} sm={6}>
          <Button
            fullWidth
            variant="outlined"
            size="large"
            startIcon={<Schedule />}
            onClick={() => navigate('/schedule')}
            sx={{
              py: 2,
              textTransform: 'none',
              fontWeight: 500,
              borderColor: theme.palette.divider,
              color: theme.palette.text.primary,
              '&:hover': {
                borderColor: theme.palette.warning.main,
                backgroundColor: `${theme.palette.warning.main}08`,
                color: theme.palette.warning.main
              }
            }}
          >
            Schedule Meeting
          </Button>
        </Grid>
        <Grid item xs={12} sm={6}>
          <Button
            fullWidth
            variant="outlined"
            size="large"
            startIcon={<CalendarMonth />}
            onClick={() => navigate('/calendar')}
            sx={{
              py: 2,
              textTransform: 'none',
              fontWeight: 500,
              borderColor: theme.palette.divider,
              color: theme.palette.text.primary,
              '&:hover': {
                borderColor: theme.palette.secondary.main,
                backgroundColor: `${theme.palette.secondary.main}08`,
                color: theme.palette.secondary.main
              }
            }}
          >
            View Calendar
          </Button>
        </Grid>
      </Grid>


      {/* Join Meeting Dialog */}
      <Dialog 
        open={joinDialog} 
        onClose={() => setJoinDialog(false)} 
        maxWidth="sm" 
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            boxShadow: '0 20px 40px rgba(0,0,0,0.1)'
          }
        }}
        slotProps={{
          backdrop: {
            sx: {
              backgroundColor: 'rgba(0, 0, 0, 0.5)',
              backdropFilter: 'blur(8px)',
              WebkitBackdropFilter: 'blur(8px)', // Safari support
            }
          }
        }}
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          pb: 1,
          borderBottom: '1px solid',
          borderColor: 'divider'
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Box
              sx={{
                width: 40,
                height: 40,
                borderRadius: 1,
                backgroundColor: `${theme.palette.primary.main}15`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <Launch sx={{ color: theme.palette.primary.main, fontSize: 20 }} />
            </Box>
            <Typography variant="h6" fontWeight={600}>
              Join Meeting
            </Typography>
          </Box>
          <IconButton 
            onClick={() => setJoinDialog(false)} 
            size="small"
            sx={{ 
              color: theme.palette.text.secondary,
              '&:hover': { backgroundColor: theme.palette.action.hover }
            }}
          >
            <Close />
          </IconButton>
        </DialogTitle>

        <DialogContent sx={{ pt: 3 }}>
          <TextField
            autoFocus
            fullWidth
            label="Meeting ID or Link"
            placeholder="Enter meeting ID or paste meeting link"
            value={meetingId}
            onChange={(e) => setMeetingId(e.target.value)}
            variant="outlined"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Link sx={{ color: 'action.active' }} />
                </InputAdornment>
              )
            }}
            sx={{ mb: 2, mt:3}}
          />
          <Typography variant="body2" color="text.secondary">
            Enter a meeting ID (e.g., 123-456-789) or paste a complete meeting link
          </Typography>
        </DialogContent>

        <DialogActions sx={{ p: 3, pt: 1, gap: 1 }}>
          <Button 
            onClick={() => setJoinDialog(false)}
            sx={{ textTransform: 'none' }}
          >
            Cancel
          </Button>
          <Button 
            variant="contained" 
            onClick={handleJoinMeeting}
            disabled={!meetingId.trim()}
            sx={{ 
              textTransform: 'none',
              px: 3
            }}
          >
            Join Meeting
          </Button>
        </DialogActions>
      </Dialog>

      {/* Share Meeting Dialog */}
      <Dialog 
        open={shareDialog} 
        onClose={() => setShareDialog(false)} 
        maxWidth="sm" 
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            boxShadow: '0 20px 40px rgba(0,0,0,0.1)'
          }
        }}
        slotProps={{
          backdrop: {
            sx: {
              backgroundColor: 'rgba(0, 0, 0, 0.5)',
              backdropFilter: 'blur(8px)',
              WebkitBackdropFilter: 'blur(8px)', // Safari support
            }
          }
        }}
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          pb: 1,
          borderBottom: '1px solid',
          borderColor: 'divider'
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Box
              sx={{
                width: 40,
                height: 40,
                borderRadius: 1,
                backgroundColor: `${theme.palette.success.main}15`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <VideoCall sx={{ color: theme.palette.success.main, fontSize: 20 }} />
            </Box>
            <Typography variant="h6" fontWeight={600}>
              Meeting Created
            </Typography>
          </Box>
          <IconButton 
            onClick={() => setShareDialog(false)} 
            size="small"
            sx={{ 
              color: theme.palette.text.secondary,
              '&:hover': { backgroundColor: theme.palette.action.hover }
            }}
          >
            <Close />
          </IconButton>
        </DialogTitle>

        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body1" sx={{ mb: 3, fontWeight: 500 }}>
            Your meeting is ready. Share this link with participants:
          </Typography>
          
          <TextField
            fullWidth
            value={generatedLink}
            variant="outlined"
            InputProps={{
              readOnly: true,
              endAdornment: (
                <InputAdornment position="end">
                  <Tooltip title="Copy Link">
                    <IconButton onClick={handleCopyLink} edge="end">
                      <ContentCopy />
                    </IconButton>
                  </Tooltip>
                </InputAdornment>
              )
            }}
            sx={{ mb: 3 }}
          />

          <Stack direction="row" spacing={1} flexWrap="wrap">
            <Chip
              icon={<ContentCopy />}
              label="Copy Link"
              onClick={handleCopyLink}
              clickable
              variant="outlined"
              sx={{ 
                borderColor: theme.palette.primary.main,
                color: theme.palette.primary.main,
                '&:hover': { backgroundColor: `${theme.palette.primary.main}08` }
              }}
            />
            <Chip
              icon={<Share />}
              label="Share"
              onClick={handleShareLink}
              clickable
              variant="outlined"
              sx={{ 
                borderColor: theme.palette.secondary.main,
                color: theme.palette.secondary.main,
                '&:hover': { backgroundColor: `${theme.palette.secondary.main}08` }
              }}
            />
            <Chip
              icon={<QrCode />}
              label="QR Code"
              clickable
              variant="outlined"
              sx={{ 
                borderColor: theme.palette.warning.main,
                color: theme.palette.warning.main,
                '&:hover': { backgroundColor: `${theme.palette.warning.main}08` }
              }}
            />
          </Stack>
        </DialogContent>

        <DialogActions sx={{ p: 3, pt: 1, gap: 1 }}>
          <Button 
            onClick={() => setShareDialog(false)}
            sx={{ textTransform: 'none' }}
          >
            Close
          </Button>
          <Button 
            variant="contained" 
            onClick={() => {
              navigate(`/meeting/${generatedLink.split('/meeting/')[1]}`);
              setShareDialog(false);
            }}
            sx={{ 
              textTransform: 'none',
              px: 3
            }}
          >
            Start Meeting
          </Button>
        </DialogActions>
      </Dialog>

      {/* Snackbar for notifications */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={3000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert 
          severity={snackbar.severity} 
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          sx={{ 
            borderRadius: 2,
            fontWeight: 500
          }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default QuickActions;