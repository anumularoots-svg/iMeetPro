// // Images index file - exports all images used in the app
// // This file provides a centralized way to import and use images

// // User & Profile Images
// export { default as DefaultAvatar } from './default-avatar.png';

// // Background Images
// export { default as MeetingBackground } from './meeting-bg.jpg';
// export { default as LoginBackground } from './login-bg.jpg';
// export { default as WaitingRoomBg } from './waiting-room-bg.jpg';

// // Illustrations & Graphics
// export { default as EmptyState } from './empty-state.svg';
// export { default as ErrorIllustration } from './error-illustration.svg';
// export { default as SuccessIllustration } from './success-illustration.svg';

// // Loading & Animation
// export { default as LoadingAnimation } from './loading.gif';

// // Usage example:
// // import { DefaultAvatar, MeetingBackground } from '@assets/images';
// // 
// // function ProfileComponent() {
// //   return (
// //     <div>
// //       <img src={DefaultAvatar} alt="User Avatar" />
// //       <div style={{ backgroundImage: `url(${MeetingBackground})` }}>
// //         Meeting Content
// //       </div>
// //     </div>
// //   );
// // }

// // Image preloading utility
// export const preloadImages = () => {
//   const images = [
//     DefaultAvatar,
//     MeetingBackground,
//     LoginBackground,
//     WaitingRoomBg,
//     EmptyState,
//     ErrorIllustration,
//     SuccessIllustration,
//     LoadingAnimation,
//   ];

//   images.forEach((src) => {
//     const img = new Image();
//     img.src = src;
//   });
// };

// // Image optimization utilities
// export const getOptimizedImageUrl = (src, width, height, quality = 80) => {
//   // This function can be extended to work with image optimization services
//   // like Cloudinary, ImageKit, or custom optimization endpoints
//   if (!src) return DefaultAvatar;
  
//   // For now, return the original image
//   // In production, you might use:
//   // return `${src}?w=${width}&h=${height}&q=${quality}`;
//   return src;
// };

// // Responsive image helper
// export const getResponsiveImageSrc = (baseSrc) => {
//   return {
//     src: baseSrc,
//     srcSet: `
//       ${baseSrc} 1x,
//       ${baseSrc.replace('.', '@2x.')} 2x,
//       ${baseSrc.replace('.', '@3x.')} 3x
//     `,
//   };
// };

// // Image constants
// export const IMAGE_FORMATS = {
//   JPEG: 'image/jpeg',
//   PNG: 'image/png',
//   SVG: 'image/svg+xml',
//   WEBP: 'image/webp',
//   GIF: 'image/gif',
// };

// export const IMAGE_SIZES = {
//   avatar: {
//     small: { width: 32, height: 32 },
//     medium: { width: 48, height: 48 },
//     large: { width: 64, height: 64 },
//     xlarge: { width: 128, height: 128 },
//   },
//   thumbnail: {
//     small: { width: 150, height: 100 },
//     medium: { width: 300, height: 200 },
//     large: { width: 600, height: 400 },
//   },
//   hero: {
//     mobile: { width: 768, height: 432 },
//     tablet: { width: 1024, height: 576 },
//     desktop: { width: 1920, height: 1080 },
//   },
// };

// // Lazy loading helper for images
// export const createLazyImageObserver = (callback) => {
//   if ('IntersectionObserver' in window) {
//     return new IntersectionObserver((entries) => {
//       entries.forEach((entry) => {
//         if (entry.isIntersecting) {
//           callback(entry.target);
//         }
//       });
//     }, {
//       rootMargin: '50px',
//     });
//   }
//   return null;
// };

// Image assets exports
// For now, we'll use placeholder imports until you have actual image files

// You can replace these with actual image imports when you have the files
export const defaultAvatar = '/src/assets/images/default-avatar.png';
export const meetingBg = '/src/assets/images/meeting-bg.jpg';
export const loginBg = '/src/assets/images/login-bg.jpg';
export const emptyState = '/src/assets/images/empty-state.svg';
export const loadingGif = '/src/assets/images/loading.gif';
export const waitingRoomBg = '/src/assets/images/waiting-room-bg.jpg';
export const errorIllustration = '/src/assets/images/error-illustration.svg';
export const successIllustration = '/src/assets/images/success-illustration.svg';

// Named export for ErrorBoundary compatibility
export const ErrorIllustration = errorIllustration;

// Default export if needed
export default {
  defaultAvatar,
  meetingBg,
  loginBg,
  emptyState,
  loadingGif,
  waitingRoomBg,
  errorIllustration,
  ErrorIllustration,
  successIllustration
};